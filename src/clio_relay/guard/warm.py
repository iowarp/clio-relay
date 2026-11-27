"""The warm allocation (RL26).

A ``run`` whose executable is ``clio-relay-warm`` asks for a held scheduler
job. It goes through the spend gate like any run. Relay submits a holder in
its place, and ``exec`` reaches the held nodes with
``srun --jobid=<native id> --overlap <command>``.

The holder releases the allocation once ten minutes have passed since anything
last used it. It measures that by the clock from the latest of: a step it sees
running (``squeue -s``), the end time of the latest finished step (``sacct``,
so a short ``srun`` that ran between two looks still counts), and the time the
job directory's ``.relay-warm-active`` file last changed. A running ``srun``
keeps it, and the holder never stops anything that is running: it only lets its
own batch script end once nothing else is. When the scheduler does not answer,
it counts the time as busy.

Without Slurm, only the marker counts: ``touch`` it to keep the allocation
(open sites only, since ``touch`` is not read-only).
"""

from __future__ import annotations

from clio_relay.jobs.spec import JobSpec

WARM_EXECUTABLE = "clio-relay-warm"
POLL_S = 30

HOLDER = r"""limit=$1 step=$2
marker="$PWD/.relay-warm-active"
job=${SLURM_JOB_ID:-${PBS_JOBID:-}}
echo "relay warm allocation $job: held; released after $limit s with nothing running"
slurm() { [ -n "${SLURM_JOB_ID:-}" ] && command -v squeue >/dev/null 2>&1; }
busy() {
  slurm || return 1
  steps=$(squeue -h -s -j "$SLURM_JOB_ID" -o %i 2>/dev/null) || return 0
  printf '%s\n' "$steps" | grep -Eq '\.[0-9]+(\+[0-9]+)?$'
}
used() {
  if [ -e "$marker" ]; then
    stat -c %Y "$marker" 2>/dev/null || stat -f %m "$marker" 2>/dev/null
  fi
  if slurm && command -v sacct >/dev/null 2>&1; then
    sacct -n -P -j "$SLURM_JOB_ID" -o JobID,End 2>/dev/null |
      while IFS='|' read -r id end; do
        case $id in *.[0-9]*) ;; *) continue ;; esac
        case $end in ''|Unknown|None) continue ;; esac
        date -d "$end" +%s 2>/dev/null || true
      done
  fi
}
last=$(date +%s)
while :; do
  now=$(date +%s)
  if busy; then last=$now; fi
  for t in $(used); do [ "$t" -gt "$last" ] && last=$t; done
  if [ $((now - last)) -ge "$limit" ] && ! busy; then
    echo "relay warm allocation $job: idle for $((now - last)) s; released at $(date -u +%FT%TZ)"
    exit 0
  fi
  sleep "$step"
done
"""


def is_warm(spec: JobSpec) -> bool:
    """Return whether ``spec`` asks for a warm allocation."""
    return spec.executable == WARM_EXECUTABLE and not spec.steps()


def holder_job(spec: JobSpec, *, idle_s: int, poll_s: int = POLL_S) -> JobSpec:
    """Return the job relay submits for a warm allocation request."""
    data = spec.model_dump(mode="json")
    data["executable"] = "/bin/sh"
    data["arguments"] = ["-c", HOLDER, WARM_EXECUTABLE, str(idle_s), str(poll_s)]
    data["name"] = spec.name or "relay-warm"
    return JobSpec.model_validate(data)
