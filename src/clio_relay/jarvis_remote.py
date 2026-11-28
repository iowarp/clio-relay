"""A JARVIS package job on a site, the same for every adapter that runs a shell there.

A package job is JARVIS's (RL40, RL45): the adapter puts the pipeline and the
site script (:mod:`clio_relay.jarvis_submit`) in the job directory and runs the
script's ``submit``, ``check`` or ``plan`` there; status, cancel, stop, move and
the service runtimes are ``jarvis execution`` commands in the job directory.
The scripts and the reading of their answers live here once. An adapter passes
its own way to run a POSIX ``sh`` script on the site, ``run(script, what)``
returning its stdout: one ``ssh`` call, or the adapter's ``exec``. The scripts
print sections that start with an ``@@name`` line (:func:`parse_sections`).
"""

from __future__ import annotations

import posixpath
import re
import shlex
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from clio_relay import jarvis_exits, jarvis_plan, jarvis_submit
from clio_relay.jobs.handles import JobHandle, JobState, JobStatus
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec

Runner = Callable[[str, str], Awaitable[str]]
"""``run(script, what)``: run ``script`` with ``sh`` on the site and return its stdout."""

q = shlex.quote


def parse_sections(text: str) -> dict[str, list[str]]:
    """Split ``@@name value`` sections. The value and the lines after it are the body."""
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in text.splitlines():
        if line.startswith("@@"):
            key, _, rest = line[2:].partition(" ")
            current = sections.setdefault(key, [])
            if rest:
                current.append(rest)
            continue
        if current is not None:
            current.append(line)
    return sections


def one(sections: dict[str, list[str]], key: str) -> str | None:
    """The first non-blank value of a section."""
    values = [value.strip() for value in sections.get(key, []) if value.strip()]
    return values[0] if values else None


# On NFS a file another node wrote can stay invisible to ``[ -f ]``, ``cat`` and
# ``stat`` for the attribute cache's lifetime (two minutes on some sites). Reading
# the directory makes the client look again, so every check of a file another
# host may have written lists its directory first.
FRESH = 'fresh() { for d in "$@"; do ls -f "$d" >/dev/null 2>&1; done; }\n'

# Enter the job directory, looking again at its parent when it seems missing.
ENTER_JOB = (
    FRESH
    + 'cd "$J" 2>/dev/null || { fresh "$(dirname "$J")"; cd "$J" 2>/dev/null; } '
    + '|| { echo "@@nodir"; exit 0; }\nfresh .\n'
)


def job_root(job_dir: str) -> str:
    """The site's JARVIS root, for a job directory ``<CLIO site home>/jobs/<operation>``."""
    return jarvis_submit.site_root(posixpath.dirname(posixpath.dirname(job_dir)))


def document(
    spec: JobSpec,
    operation_id: str,
    job_dir: str,
    *,
    scheduler: str,
    job_name: str,
    threads_per_core: int | None = None,
) -> dict[str, object]:
    """The job's pipeline for the site's ``scheduler`` (:func:`jarvis_submit.scheduler_block`),
    a relative ``stdout_path`` or ``stderr_path`` being in the job directory."""

    def _in_job(value: str | None) -> str | None:
        return None if value is None else posixpath.join(job_dir, value.replace("\\", "/"))

    block = jarvis_submit.scheduler_block(
        spec,
        scheduler=scheduler,
        job_name=job_name,
        stdout=_in_job(spec.stdout_path),
        stderr=_in_job(spec.stderr_path),
        job_dir=job_dir,
        threads_per_core=threads_per_core,
    )
    return jarvis_submit.pipeline_document(
        spec, pipeline=jarvis_submit.pipeline_id(operation_id), scheduler=block, job_dir=job_dir
    )


def submit_script(
    job_dir: str, command: str, links: Sequence[tuple[str, str]], *, keep: bool
) -> str:
    """Run the site script's ``command`` in the job directory, the inputs linked first.

    ``links`` are ``(name, path)``: each input's path on the site, linked (else
    copied) into the job directory as its name. With ``keep`` (a submit) the answer
    is kept as ``submit.out`` and ``submit.rc``, and a submit that already exited 0
    prints that answer again rather than submitting twice. Prints ``@@rc``, the
    output under ``@@out`` and stderr under ``@@err``.
    """
    linked = "".join(
        f't={q(name)}; mkdir -p "$(dirname "$t")"; rm -f "$t"; '
        f'ln {q(path)} "$t" 2>/dev/null || cp {q(path)} "$t" || exit 66\n'
        for name, path in links
    )
    run = f"( {linked}{command} ) </dev/null >submit.out.$$ 2>submit.err\nrc=$?\n"
    if keep:
        run = (
            'if [ "$(cat submit.rc 2>/dev/null)" = 0 ]; then rc=0; : > submit.err\nelse\n'
            + run
            + "mv -f submit.out.$$ submit.out; echo $rc > submit.rc\nfi\n"
        )
    else:
        run += "mv -f submit.out.$$ submit.out\n"
    return (
        f"J={q(job_dir)}\n"
        + 'mkdir -p "$J" && cd "$J" || { echo "@@nodir"; exit 0; }\n'
        + run
        + 'echo "@@rc $rc"; echo "@@out"; cat submit.out; echo; echo "@@err"; cat submit.err\n'
        + "exit 0\n"
    )


def submitted(
    adapter: str,
    sections: dict[str, list[str]],
    *,
    mode: jarvis_submit.SiteMode,
    spec: JobSpec,
    site: str,
    install_walltime_s: int | None,
    data: dict[str, Any],
) -> JobHandle | dict[str, Any] | None:
    """Read the site script's answer to ``submit``, ``check`` or ``plan``.

    A plan is JARVIS's plan or None (a refusal is not one: the submit says why).
    A typed exit becomes its refusal (:func:`jarvis_exits.refuse`). A check returns
    JARVIS's install report and a submit the job's handle; ``data`` is the
    adapter's own handle data, with ``root`` and ``job_dir``.
    """
    rc = one(sections, "rc")
    output = "\n".join(sections.get("out", [])).strip()
    if mode == "plan":
        return jarvis_plan.planned(rc, output)
    if rc != "0":
        error = "\n".join(sections.get("err", [])).strip() or output
        jarvis_exits.refuse(
            rc, output, error, spec=spec, site=site, install_walltime_s=install_walltime_s
        )
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            f"{site} refused the job: {jarvis_exits.failure_text(output, error) or 'no output'}",
            scheduler="jarvis",
            exit_code=int(rc) if rc and rc.lstrip("-").isdigit() else None,
        )
    if mode == "check":
        return jarvis_exits.checked(output)
    return handle(adapter, output, data)


def handle(adapter: str, submit_output: str, data: dict[str, Any]) -> JobHandle:
    """The handle of the job ``ppl submit`` printed; ``data`` has ``root`` and ``job_dir``."""
    found = jarvis_submit.parse_handle(submit_output)
    if found is None:
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            "jarvis ppl submit printed no execution handle: "
            f"{submit_output.strip()[-500:] or 'no output'}",
            scheduler="jarvis",
        )
    native, jarvis_data = jarvis_submit.handle_data(
        found,
        job_dir=str(data["job_dir"]),
        root=jarvis_submit.site_root(str(data["root"])),
        shared=jarvis_submit.shared_dir(submit_output),
    )
    if (plan := jarvis_plan.parse(submit_output)) is not None:
        jarvis_data[jarvis_plan.KEY] = plan
    return JobHandle(adapter=adapter, native_id=native, data={**data, **jarvis_data})


def status_script(
    job_dir: str,
    execution_id: str,
    pipeline: str | None = None,
    *,
    states: str | None = None,
) -> str:
    """One poll of a job JARVIS submitted: ``jarvis execution get``, and its artifacts.

    ``execution get`` reconciles the record with the scheduler and commits it,
    so relay keeps no state of its own: a pending job's place in the queue is
    the record's ``metadata.queue``, and a job that left the queue unseen is
    ended by JARVIS. The artifacts are read once the record is terminal.
    ``pipeline`` is as in :func:`jarvis_submit.execution_argv`. ``states`` is
    JARVIS's ``components.jsonl``, printed under ``@@components`` after ``get``.
    """
    root = job_root(job_dir)
    get = jarvis_submit.execution_shell("get", execution_id, root=root, pipeline=pipeline)
    artifacts = jarvis_submit.execution_shell(
        "artifacts", execution_id, root=root, pipeline=pipeline
    )
    execution = q(jarvis_submit.execution_dir(root, execution_id, pipeline=pipeline))
    return (
        f"J={q(job_dir)}\n"
        + ENTER_JOB
        + 'echo "@@inputs"; cat inputs.json 2>/dev/null; echo\n'
        + '[ -e inputs.dropped ] && echo "@@dropped"\n'
        + f"fresh {execution}; jarvis=$(cat {jarvis_submit.JARVIS_FILE} 2>/dev/null)\n"
        + '[ -n "$jarvis" ] || { echo "@@nojarvis"; exit 0; }\n'
        + f"out=$({get} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@getrc $rc"; echo "@@get"; printf "%s\\n" "$out"\n'
        + (f'echo "@@components"; cat {q(states)} 2>/dev/null; echo\n' if states else "")
        + 'if [ "$rc" -eq 0 ] && printf \'%s\\n\' "$out" | tail -n 1 | grep -qF '
        + "'\"terminal\":true'; then\n"
        + f'  echo "@@artifacts"; {artifacts} 2>/dev/null </dev/null; echo\n'
        + "fi\n"
        + "exit 0\n"
    )


def execution_script(
    job_dir: str,
    verb: jarvis_submit.ExecutionVerb,
    execution_id: str,
    pipeline: str,
    extra: Sequence[str] = (),
) -> str:
    """Run ``jarvis execution <verb>`` (cancel, stop, move or service-runtimes) in the job
    directory, with the verb's own options ``extra``.

    Prints ``@@rc`` and the command's output under ``@@out``; the output's last
    line is the document JARVIS printed: the execution record it committed, its
    service runtimes, or a typed refusal.
    """
    command = jarvis_submit.execution_shell(
        verb, execution_id, root=job_root(job_dir), pipeline=pipeline, extra=extra
    )
    return (
        f"J={q(job_dir)}\n"
        + ENTER_JOB
        + f"jarvis=$(cat {jarvis_submit.JARVIS_FILE} 2>/dev/null)\n"
        + '[ -n "$jarvis" ] || { echo "@@nojarvis"; exit 0; }\n'
        + f"out=$({command} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@rc $rc"; echo "@@out"; printf "%s\\n" "$out"\n'
        + "exit 0\n"
    )


def install_script(job_dir: str, install_command: str) -> str:
    """Submit the install job from the job directory: the site script's ``install`` mode.

    Prints ``@@rc`` and the output under ``@@out``; the handle line JARVIS printed
    is kept as ``relay.install.json`` and printed under ``@@handle``.
    """
    return (
        f"J={q(job_dir)}\n"
        + ENTER_JOB
        + f"out=$({install_command} 2>&1 </dev/null); rc=$?\n"
        + 'echo "@@rc $rc"; echo "@@out"; printf "%s\\n" "$out"\n'
        + f'echo "@@handle"; cat {jarvis_submit.INSTALL_FILE} 2>/dev/null; echo\n'
        + "exit 0\n"
    )


def installed(
    adapter: str,
    text: str,
    *,
    spec: JobSpec,
    site: str,
    install_walltime_s: int | None,
    login_mode: str,
    data: dict[str, Any],
) -> JobHandle:
    """Read :func:`install_script`'s answer: the install job's handle, or none needed.

    ``login_mode`` is the mode of a build JARVIS started on the submitting host
    (:func:`jarvis_exits.login_build`); ``data`` has ``root`` and ``job_dir``.
    """
    found = parse_sections(text)
    job_dir = str(data["job_dir"])
    if "nodir" in found:
        raise Refusal(
            RefusalReason.INPUT_MISSING,
            f"the job directory {job_dir} is gone from {site}; run the job again",
        )
    output, rc = "\n".join(found.get("out", [])).strip(), one(found, "rc")
    jarvis_exits.refuse(
        rc,
        output,
        output,
        spec=spec,
        site=site,
        install_walltime_s=install_walltime_s,
        what="the install",
    )
    printed = jarvis_submit.parse_handle("\n".join(found.get("handle", [])))
    base = {key: value for key, value in data.items() if key != "job_dir"}
    base["install"] = True
    if here := jarvis_exits.without_job(adapter, rc, output, printed, login_mode, base):
        return here
    if rc != "0" or printed is None:
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            f"{site} refused the install job: {output[-500:] or 'no output'}",
            scheduler="jarvis",
            exit_code=int(rc) if rc and rc.lstrip("-").isdigit() else None,
        )
    native, jarvis_data = jarvis_submit.handle_data(
        printed,
        job_dir=job_dir,
        root=jarvis_submit.site_root(str(data["root"])),
        shared=jarvis_submit.shared_dir(output),
    )
    return JobHandle(adapter=adapter, native_id=native, data={**jarvis_data, **base})


def _lost(job_dir: str, site: str) -> JobStatus:
    return JobStatus(
        state=JobState.FAILED,
        native_state="lost",
        message=f"the job directory {job_dir} is gone from {site}",
    )


def _unnamed(job_dir: str, site: str) -> Refusal:
    return Refusal(
        RefusalReason.UNREACHABLE,
        f"the job directory {job_dir} on {site} does not say which JARVIS submitted the job",
    )


async def status(
    run: Runner, handle: JobHandle, *, site: str
) -> tuple[JobStatus, dict[str, list[str]]]:
    """Read the job from JARVIS's execution record; relay keeps no state of its own.

    Returns the status and the poll's sections, for an adapter that keeps
    inputs of its own (``@@inputs``, ``@@dropped``).
    """
    job_dir = str(handle.data["job_dir"])
    execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
    execution = str(handle.data.get("execution_dir") or "") or jarvis_submit.execution_dir(
        jarvis_submit.site_root(str(handle.data["root"])), execution_id, pipeline=pipeline
    )
    states = None if handle.data.get("install") else f"{execution}/{jarvis_plan.STATES_FILE}"
    sections = parse_sections(
        await run(status_script(job_dir, execution_id, pipeline, states=states), "status")
    )
    if "nodir" in sections:
        return _lost(job_dir, site), sections
    if "nojarvis" in sections:
        raise _unnamed(job_dir, site)
    answer = "\n".join(sections.get("get", []))
    record = jarvis_submit.parse_document(answer, jarvis_submit.RECORD_SCHEMA)
    if one(sections, "getrc") != "0" or record is None:
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"jarvis execution get did not answer on {site}: "
            f"{answer.strip()[-500:] or 'no output'}",
        )
    found = jarvis_submit.record_status(record, job_id=handle.native_id)
    found.components = jarvis_plan.component_states("\n".join(sections.get("components", [])))
    if not found.state.final or handle.data.get("install"):
        return found, sections
    snapshot = jarvis_submit.parse_document(
        "\n".join(sections.get("artifacts", [])), jarvis_submit.ARTIFACTS_SCHEMA
    )
    if snapshot is not None:
        found.outputs = jarvis_submit.artifact_outputs(snapshot, execution=execution)
    return found, sections


async def control(
    run: Runner,
    handle: JobHandle,
    verb: jarvis_submit.ExecutionVerb,
    observe: Callable[[JobHandle], Awaitable[JobStatus]],
) -> JobStatus:
    """``jarvis execution cancel|stop`` for a job that has not ended; ``observe`` is status."""
    current = await observe(handle)
    if current.state.final:
        return current
    execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
    sections = parse_sections(
        await run(execution_script(str(handle.data["job_dir"]), verb, execution_id, pipeline), verb)
    )
    if "nodir" in sections or "nojarvis" in sections:
        return await observe(handle)
    if one(sections, "rc") != "0":
        current = await observe(handle)
        if current.state.final:
            return current
        raise jarvis_submit.execution_failed(verb, "\n".join(sections.get("out", [])))
    return await observe(handle)


async def move(run: Runner, handle: JobHandle, queue_name: str, *, site: str) -> None:
    """Move a pending job to ``queue_name`` with ``jarvis execution move``.

    JARVIS checks that the job still waits, that the queue exists and that
    the job fits there, and refuses with typed issues (exit 5) otherwise;
    relay passes those on (:func:`jarvis_submit.jarvis_refused`).
    """
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", queue_name):
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"{queue_name!r} is not a queue name")
    execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
    job_dir = str(handle.data["job_dir"])
    sections = parse_sections(
        await run(
            execution_script(
                job_dir, "move", execution_id, pipeline, extra=["--queue", queue_name]
            ),
            "moving the job",
        )
    )
    rc = one(sections, "rc")
    output = "\n".join(sections.get("out", [])).strip()
    if "nojarvis" in sections:
        raise _unnamed(job_dir, site)
    if rc == str(jarvis_submit.EXIT_REFUSED):
        raise jarvis_submit.jarvis_refused(
            output, site=site, what=f"moving the job to {queue_name}"
        )
    if rc != "0":
        raise Refusal(
            RefusalReason.SUBMIT_FAILED,
            f"jarvis execution move did not move the job to {queue_name}: "
            f"{output[-500:] or 'no output'}",
            queue=queue_name,
        )


async def service_runtimes(run: Runner, handle: JobHandle) -> dict[str, Any] | None:
    """``jarvis execution service-runtimes +json`` for a JARVIS job, read on the site."""
    if handle.data.get("mode") != "jarvis" or not handle.data.get("job_dir"):
        return None
    execution_id, pipeline = jarvis_submit.handle_pipeline(handle.data)
    sections = parse_sections(
        await run(
            execution_script(
                str(handle.data["job_dir"]), "service-runtimes", execution_id, pipeline
            ),
            "reading the service runtimes",
        )
    )
    if one(sections, "rc") != "0":
        return None
    return jarvis_submit.parse_document(
        "\n".join(sections.get("out", [])), jarvis_submit.SERVICE_RUNTIMES_SCHEMA
    )
