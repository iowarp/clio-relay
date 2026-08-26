"""clio-relay#265 + #183 residual: the application-level verdict, DISTINCT
from the scheduler/launcher-only answer ``execution_watch.
execution_watch_succeeded`` gives.

Split out of ``execution_watch.py`` (adversarial-review item 2: that module
crossed its own 800-line cap once Ruling A's ``returncode_conflict``
handling landed -- a first-time crossing gets a real owner-module split,
never a new baseline exemption). ``execution_watch.py`` re-exports every
name here under its original spelling, so every existing
``execution_watch.application_verdict_for_metadata`` /
``execution_watch.APPLICATION_VERDICT_SCHEMA`` /
``execution_watch.RETURNCODE_CONFLICT_REASON`` access (including this
repo's own test suite) keeps resolving unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clio_relay.runtime_metadata import JarvisRuntimeMetadata

#: clio-relay#265 + #183 residual: the typed, DISTINCT application-level
#: verdict schema -- see :func:`application_verdict_for_metadata`.
APPLICATION_VERDICT_SCHEMA = "clio-relay.application-verdict.v1"
#: Typed reason when JARVIS's own state and returncode fields disagree
#: (state=="completed" with a nonzero/otherwise-conflicting returncode) --
#: adversarial-review Ruling A.
RETURNCODE_CONFLICT_REASON = "returncode_conflict"

#: JARVIS's own terminal execution states mapped to #265's application
#: verdict status vocabulary. Anything not taught here (a future JARVIS
#: state) reports "unknown" with a typed ``jarvis_state:<raw>`` reason
#: rather than being silently guessed at -- the same closed-vocabulary
#: discipline ``execution_watch._NON_TERMINAL_STATE_PHASE`` already uses.
_TERMINAL_STATE_APPLICATION_STATUS: dict[str, str] = {
    "completed": "success",
    "failed": "failed",
    "canceled": "failed",
}


def application_verdict_for_metadata(metadata: JarvisRuntimeMetadata) -> dict[str, object]:
    """Build #265's typed, DISTINCT application-level verdict.

    ``execution_watch_succeeded`` answers one narrow question -- did
    JARVIS's own execution record reach ``state == "completed"`` -- the
    scheduler/launcher's own outcome (did mpirun/SLURM exit cleanly). #265's
    own issue text names why that is not the same question as "did the
    application do its work": "a crashed-after-startup run, a 0-step run,
    or an empty-output run can all exit 0". This verdict is additive,
    carried ALONGSIDE ``execution_watch_succeeded``'s own state-only answer
    rather than replacing it -- it names the limitation explicitly for a
    run card to render honestly (D1's ``outputs_missing`` check is the
    other, complementary half: it inspects the declared OUTPUTS this
    function cannot see). Unlike an earlier revision, this function DOES
    read ``returncode``, not only ``state`` -- see the ``returncode_
    conflict`` branch below -- and ``execution_watch.resolve_execution_
    outcome`` DOES fold a ``returncode_conflict`` verdict into the job's
    terminal outcome (adversarial-review Ruling A: "make it earn its name";
    defense-in-depth for loose/legacy metadata the native-document
    contract's own cross-field validator does not cover). ``status`` is
    ``"unknown"`` whenever JARVIS has not (yet, or ever) reported a
    resolvable terminal outcome -- never fabricated from a state JARVIS did
    not report.
    """
    state = metadata.terminal.state
    returncode = metadata.terminal.returncode
    if metadata.terminal.terminal is not True or state is None:
        return {
            "schema_version": APPLICATION_VERDICT_SCHEMA,
            "status": "unknown",
            "application_returncode": returncode,
            "reason": "execution_not_terminal",
        }
    status = _TERMINAL_STATE_APPLICATION_STATUS.get(state)
    if status is None:
        return {
            "schema_version": APPLICATION_VERDICT_SCHEMA,
            "status": "unknown",
            "application_returncode": returncode,
            "reason": f"jarvis_state:{state}",
        }
    if status == "success" and returncode not in (0, None):
        # Adversarial-review fix (Ruling A): a self-contradicting shape --
        # JARVIS reported state=="completed" (the launcher exited cleanly)
        # but its own returncode field disagrees. The strict native-
        # document contract's cross-field validator forbids exactly this
        # (`state == "completed" and return_code != 0` raises, see
        # runtime_metadata_native_documents.py) for the main watched path,
        # but that validator never sees looser/legacy-coerced metadata
        # (``runtime_metadata_coercion.py``) -- this branch is what makes
        # THIS function earn its name there too: it reads returncode, not
        # only state, so it never reports the self-contradiction
        # "status: success, application_returncode: 3" a caller could
        # otherwise be handed.
        return {
            "schema_version": APPLICATION_VERDICT_SCHEMA,
            "status": "failed",
            "application_returncode": returncode,
            "reason": RETURNCODE_CONFLICT_REASON,
        }
    failure_reason = metadata.terminal.reason or f"jarvis_state:{state}"
    return {
        "schema_version": APPLICATION_VERDICT_SCHEMA,
        "status": status,
        "application_returncode": returncode,
        "reason": None if status == "success" else failure_reason,
    }
