# Adding a machine or an adapter

Relay reaches a computer through an adapter: ssh, Tapis, Globus Compute, the
IRI Facility API, SkyPilot or relay's own worker. There are two ways to bring
a new facility to relay, and you need only one of them:

- **A new machine on a protocol relay already speaks.** If you can ssh to it,
  or it is behind Tapis, Globus Compute or IRI, add it as a site. No code.
- **A new way in.** If the facility has its own API (a REST service, a job
  gateway, a transfer service), write an adapter for it: one Python class in
  your own package. Relay's code does not change.

## Add a machine on an existing protocol

A site is one entry in `sites.yaml`: an id, the adapter, the adapter's
settings, and a policy.

```powershell
clio-relay adapters                       # the adapters installed, and the verbs each one supports
clio-relay site add my-cluster --adapter ssh --config '{"ssh_profile": "my-cluster"}' --policy open
clio-relay describe my-cluster            # check that relay reaches it
```

Each adapter's page lists its settings: [ssh](adapters/ssh.md),
[tapis](adapters/tapis.md), [globus](adapters/globus.md), [iri](adapters/iri.md),
[sky](adapters/sky.md), [worker](adapters/worker.md).

To offer the machine to everyone who installs relay, add it to the catalog
instead. The catalog is the list `clio-relay site catalog` shows:

1. Add `src/clio_relay/catalog/sites/<id>.yaml`. It holds the same fields as a
   `sites.yaml` entry, plus a `name`, a `description` and a `source` link to
   the facility's own documentation. `ares-tapis.yaml` is a short example;
   `clio-relay site catalog --schema` prints the schema.
2. Optionally add `src/clio_relay/catalog/skills/<id>.md`, a short page that
   tells an agent what is particular about the machine: its queues, its
   accounts, its file systems. Name it in the site file's `skill` field.
3. Run `uv run pytest tests/relay_next/test_catalog.py`, which checks every
   catalog file against the schema and its adapter's settings. Leave out a
   setting only the person can know, such as their own endpoint; anything
   the file does set must be valid.

## Write an adapter

### What you write

An adapter is a subclass of `BaseAdapter` from `clio_relay.adapters.base`.
That module is the whole contract, and everything an adapter uses is
importable from it. Its docstring lists every method and hook.

| Piece | What it is |
| --- | --- |
| `name` | The adapter's name in `sites.yaml` and in the entry point. |
| `Config` | A pydantic model of one site's settings, with `extra="forbid"`. Relay validates `sites.yaml` with it, and the Compute tab draws its form from its JSON Schema. |
| `implements` | The verbs it supports: `describe`, `run`, `exec`, `files`, `serve`. |
| `describe()` | Reports the site as a `SiteInfo`. Required; it must not change anything. A facility without a batch scheduler reports `scheduler=None` (null on the wire); `SiteInfo` turns `"none"` into null too. |
| `run()`, `status()`, `cancel()`, `find()` | Jobs, for `run`. |
| `exec()` | One bounded command, for `exec`. |
| `files()` | File access, for `files`. |
| `serve()` | A long-running service with ports, for `serve`. |
| `sign_in_needed()`, `sign_in_done()` | What the person must do before the adapter can act. |

A verb you leave out of `implements` refuses with `verb_not_implemented` by
itself. Start with `describe` and `files`, or `describe` and `run`, and add the
rest later.

### Start from the template

[`examples/adapter-template`](../examples/adapter-template) is a complete
adapter for a pretend facility, Acme, whose REST API has a token, a job queue
and a file system. A fake of that API ships with it, and the adapter passes the
conformance kit against the fake. It implements `describe`, `run`, `exec` and
`files`.

```bash
cp -r examples/adapter-template ~/relay-myfacility
cd ~/relay-myfacility
```

The copy's `pyproject.toml` points `[tool.uv.sources]` at `../..`, the relay
checkout it came from. Point it at your relay checkout or a git URL, or delete
the table to use the released `clio-relay`. Then:

```bash
uv run pytest            # all pass, before you change anything
```

Then rename `relay_acme`, `AcmeAdapter` and the entry point, and replace each
call to Acme's API with your facility's.

### Settings

Put everything that differs between two sites of your facility in `Config`:
the API's URL, the project or allocation, the work directory. Give each field
a `description`, which the Compute tab shows. Never put a secret in `Config`;
`sites.yaml` is plain text.

Then say where a config leads. The classmethod `reaches(config)` returns a
`Reach`: the place the site is (a host, or a web API's host and system, which
`api_place()` builds) and the account it acts as. Two sites on one adapter
with the same `Reach` are one endpoint, so the site store refuses the second
with `site_exists` and names the first in `existing`, and the Compute tab offers
to update it. Settings that don't change the place, such as a work directory,
stay out of `Reach`, so changing them updates the row. Return `None` when the
config names no place relay can compare; such sites are never refused as a
repeat.

### Sign-in and secrets

When the adapter cannot act yet, `sign_in_needed()` returns a `SignInNeed`,
and relay asks the person on the adapter's behalf.

| The facility uses | Return | Then |
| --- | --- | --- |
| An API key or token the person pastes | `SignInNeed(provider="keyring:<name>", message=...)` | Relay shows a page that stores the key in the person's keyring. The adapter reads it with `self.context.secret("<name>")`. |
| A browser sign-in (OAuth, a device code) | `SignInNeed(provider="<name>", url="https://...", message=...)` | Relay shows the link. When the person finishes, relay calls `sign_in_done(answer)`. |
| ssh keys, or a credential the site already has | `None` | |

A call that finds the token missing or refused raises
`Refusal(RefusalReason.NEEDS_SIGN_IN, ..., provider="keyring:<name>")`, so the
agent knows to ask the person.

### Errors

Every failure the person or the agent can do something about is a `Refusal`
with one `RefusalReason`. Agents branch on the reason, so use the one that
fits:

| Reason | When |
| --- | --- |
| `needs_sign_in` | No credential, or the facility refused it |
| `unreachable` | The facility does not answer, or answers with a server error |
| `punch_failed` | A link that must hole punch (frp `xtcp`) could not. Relay never falls back to another link |
| `invalid_config` | A site setting is wrong |
| `invalid_spec`, `invalid_arguments` | The job or the call asks for something the facility cannot do |
| `path_not_found`, `path_exists` | Files; see [files](files.md) |
| `input_missing` | A job input names a digest relay does not hold |
| `submit_failed` | The facility rejected a job |

Put what helps in the refusal's data: `Refusal(reason, message, url=...)`.
Anything else that goes wrong is a bug, and relay reports it as one.

### Files

A facility reached over a network subclasses `SiteFiles` and implements six
primitives on absolute paths. `SiteFiles` applies every rule in
[files](files.md) on top of them: relative paths, `overwrite`, chunked uploads
and resuming, delete refusals and the content store.

| Primitive | Does |
| --- | --- |
| `async kind(path) -> Kind` | Says what `path` is without following a symlink: `"file"`, `"dir"`, `"link"` or `"missing"`. A missing path is `"missing"`, not an error. |
| `async describe(path) -> FileEntry` | The entry of an existing path, following a symlink. |
| `async children(path) -> list[FileEntry]` | An existing directory's entries, sorted by name, with absolute paths. |
| `async get(path, offset, limit) -> bytes` | Up to `limit` bytes of an existing file from `offset`; all of it when `limit` is `None`. |
| `async put(path, data) -> None` | Writes `data` as the whole file, making parent directories. |
| `async remove(path) -> None` | Removes one file, symlink (never its target) or empty directory. |

A `FileEntry` has `path`, `kind` (`"file"`, `"dir"` or `"other"`), `size`
and `modified`. `Kind` is a separate, smaller question for `kind()`: it adds
`"link"` and `"missing"`, which never appear in an entry. `SiteFiles` calls a
primitive on a path only after `kind` has said it exists, so a primitive
raises `Refusal(RefusalReason.PATH_NOT_FOUND, ...)` only if the path vanished
in between.

The site needs no append and no recursive delete: chunks collect in a local
staging directory and go up in one `put` (or `upload`) when the last one
arrives, and a recursive delete removes one entry at a time. Override
`upload(path, local)` to stream a large file from disk, and
`remove_tree(path)` if the site deletes a tree in one call.

If the site caps one upload, set the class attributes `max_upload_bytes` and
`upload_hint`. The cap applies to the whole file, chunked or not: a chunked
upload refuses at the first chunk that takes it past the cap. Relay's refusal
says the file is too big and then appends `upload_hint`, so the hint names
another way to move the file, such as a transfer service.

`files()` returns one, built with `root` (the site's work directory), `home`
(the person's home directory on the site) and `staging`
(`self.context.state_dir / "parts"`). See `AcmeFiles` in the template.

### Jobs

- `run(spec, operation_id=...)` submits a job and returns a `JobHandle` at
  once. It does not wait for the job.
  - `spec` is a PSI/J job spec: `executable`, `arguments`, `environment`,
    `directory`, `resources` and `attributes` (queue, account, duration).
  - Run the job in `jobs/<operation_id>` under the work directory unless
    `spec.directory` says otherwise.
  - Each `spec.inputs` names a file by SHA-256. Read its bytes with
    `self.context.content.get(sha256)`, which refuses with `input_missing`,
    and put them in the job directory first.
  - Name the facility's job after `operation_id`, so `find` can look it up.
- `status(handle)` maps the facility's state to one of `queued`, `running`,
  `completed`, `failed` or `cancelled`. A finished job lists its output files.
  A JARVIS job also fills `components` from JARVIS's `components.jsonl`
  (`jarvis_plan.component_states`), so the card shows each package's state.
- `cancel(handle)` cancels and returns the status it then observes.
  Cancelling a finished job changes nothing.
- `find(operation_id)` returns the handle of a job relay submitted but never
  recorded, for example because relay stopped mid-call.

A handle is JSON, and relay stores it. A new adapter instance in a new process
must be able to follow and cancel the job from the handle alone: put the
facility's job id in `native_id` and anything else you need in `data`.

### Exec

`exec(command, *, timeout_s=None, max_output_bytes, on_stall=None)` runs one
command on the site and returns an `ExecResult` once it ends.

- `command` is an argv list. Run it without a shell, or quote each part.
- The result holds `argv`, `exit_code`, `stdout`, `stderr` and `duration_s`.
  Each of `stdout` and `stderr` keeps at most `max_output_bytes` bytes on its
  own; set `stdout_truncated` or `stderr_truncated` when you cut one.
- When `timeout_s` passes, stop the command on the site and return the result
  with `timed_out=True` and `exit_code=None`. A time limit is not a refusal.
- With no `timeout_s` the command runs until it ends. While it prints nothing,
  call `await on_stall(idle_s, next_s)`: the seconds since its last output,
  and the seconds until you will report again. Start after `STALL_S` (30 s)
  of quiet and report again every `STALL_S` while it stays quiet, so the
  status never goes stale. The command keeps running; the report lets relay
  tell the person it is still going.
- The person may cancel the call. Relay then cancels your coroutine, so stop
  the command on the site in an `except asyncio.CancelledError` or `finally`
  block, and let the cancellation go on.
- A program the site cannot find ends with an exit code, such as 127, and does
  not hang.

`AcmeAdapter.exec` in the template does all of this over a facility API that
starts a command, reports whether it has ended and how much it has printed,
returns the first bytes of each stream, and stops it.

### Services and ports

`serve(spec, *, operation_id)` starts the service on the site and returns its
`Endpoint`. The desktop end of each port belongs to relay, not to the adapter:

- An adapter that gives `ssh_argv()` gets an `ssh -L` forward, started and
  adopted by relay.
- Otherwise relay uses the rendezvous host. The adapter's job runs `frpc`
  beside the service with the config relay renders for it, and relay runs its
  own frp visitor (`clio_relay.frp_visitor`) on the desktop as its own
  process. That package is the whole desktop frp client: it reads the
  `visitor.toml` relay writes, a subset of frpc's format, and refuses any
  setting it doesn't run rather than ignoring it. It speaks `stcp`, and
  `xtcp` with its UDP hole punch and QUIC tunnel, falling back to `stcp`
  where the file names a `fallbackTo`. Nothing else in relay speaks
  frp's wire, and an adapter neither starts a desktop frp program nor imports
  the visitor.
- An adapter that already holds an frp link to its site, as the worker does,
  gives `serve_link()`: the link's rendezvous and the site's frpc. A service
  there then uses frp with no serve settings, and tries the direct path when
  the link does. Anything set in the serve settings wins over it.

See [rendezvous host](rendezvous-host.md) for the transports it speaks.

### Policy

Each site is `open` or `guarded`. Relay's guard sits in front of every adapter
and enforces the policy, so an adapter does not check it. A JARVIS package job
goes only to an adapter with `runs_packages = True`, which reads `spec.steps()`
(one entry for `package`, the ordered pipeline for `packages`);
relay refuses a package for any other adapter before it asks anything
(`jarvis_submit.not_run_here`). Relay never turns a package into its own command
job inside an allocation. A batch adapter hands the job to JARVIS rather than
its own scheduler call: `clio_relay.jarvis_submit` renders the pipeline with its
`scheduler:` key, the site script that runs `jarvis ppl submit`, and the parsers
that turn JARVIS's execution record into a `JobStatus` and outputs. A site
with no scheduler names `local`, PSI/J's local executor, so there is one submit
path. `clio_relay.jarvis_remote` holds the job scripts an adapter runs on the
site (submit, status, `jarvis execution` controls), given a callable that runs
shell text there, so an adapter differs only in how it runs it. The local,
worker, ssh and tapis adapters all use it.

### Optional hooks

Relay calls these when the adapter defines them:

| Hook | What it is for |
| --- | --- |
| `on_site_added(site)` | Set something up when a site is saved, such as registering a system with the facility |
| `job_name(operation_id)` | The facility's job name, which relay shows |
| `price(spec)` | What a job would cost, for the spend question on guarded sites |
| `system_id()` | The facility's own id for the site, shown in the Compute tab |
| `stop(handle)` | Stop a package service so its package's own stop runs (`jarvis execution stop`) |
| `install(spec, answers, *, operation_id)` | Submit `jarvis ppl install +submit` with the person's answers and return its handle. Pass the build its own walltime: `jarvis_submit.install_pipeline` of the run's pipeline as `install_command(pipeline=...)` |
| `jarvis_home()` | The CLIO site home, as the site's shell names it. With it and `exec`, the base class sets up the site's JARVIS root (`install_jarvis`, `jarvis_submit.setup_command`) and lists and adds its packages (`site_packages`, `jarvis_submit.packages_command`), so an adapter writes neither |
| `plan_job(spec, *, operation_id)` | Return JARVIS's plan of the package job `run` would submit, submitting nothing: run the site script in its `plan` mode (`jarvis ppl submit +plan +json`) and parse it with `jarvis_plan.parse`. Relay draws the job card from it and passes the yes back as `jarvis_plan.bound(spec, plan)`, so `run` submits with `--plan_sha256`. `None` (the default) means no plan, and the card shows the job as relay records it |
| `install_check(spec, *, operation_id)` | Run the site script in its `check` mode (`jarvis ppl install +check`) and return JARVIS's install report (`jarvis_exits.checked`). With `install`, it gives the site the `install` tool: the software ahead of any run |

A batch adapter that runs packages turns the site script's typed exits into
refusals with `jarvis_exits.refuse`. In detail, it refuses with `jarvis_report.install_needed`
when the submit exits 3 (with `walltime=jarvis_submit.install_walltime(...)`,
which is None unless the site sets an install walltime, so the approval says
how long the build may take only when relay, not JARVIS, sets it), with
`jarvis_exits.build_refused` when it exits 4
(JARVIS will not build outside an allocation; never retry it), and with
`jarvis_submit.missing_jarvis` when the site script exits 69: JARVIS is
missing or at another pin. Before those, it calls
`install_failures.raise_for(exit_code, output, site=self.context.title)`, which raises
JARVIS's failure records (exit 6, passed whole and never asked) and a
blocking `site_fact_uncertain`
question (exit 3), both with JARVIS's issues untouched. A refusal names the
site by `self.context.title`, the name the person gave it, as describe does;
it carries no `site` field. Relay turns the rest into the install question and
calls `install` or `install_jarvis` itself. A missing Spack is JARVIS's own
`spack_missing` question (exit 3), asked like any other choice. The `install`
tool's check takes the same path: exit 0 means the software is all there, and
relay completes the operation without launching anything.

An adapter whose site runs JARVIS fills `SiteInfo.partitions` (a
`PartitionInfo` per partition: nodes, CPUs per node, memory, max time and idle
or allocated counts) and `SiteInfo.mpi` from `jarvis sched info +json`, with
`clio_relay.adapters.sched_info`. Relay reads no scheduler or MPI facts
itself. The guard sizes jobs against the partitions, and a site that reports
none is not checked. `Charge.with_layout(spec)` adds the
layout (threads, memory, each step's nodes, the job's `sizing`) to a price.

### Register it

Name the class in your package's `pyproject.toml`:

```toml
[project.entry-points."clio_relay.adapters"]
myfacility = "relay_myfacility:MyFacilityAdapter"
```

Install your package next to relay (`uv tool install clio-relay --with
relay-myfacility`, or `--with /path/to/relay-myfacility` for a local
checkout), and `clio-relay adapters` lists it. In tests, or to try it
without installing, `clio_relay.adapters.registry.register(MyFacilityAdapter)`
does the same in one process.

## Test it with the conformance kit

The kit is one test class, `AdapterConformance` in
`clio_relay.testing.conformance`. Subclass it, name your adapter class in
`adapter`, and say how to build a site. The class tells the kit which verbs to
test, from its `implements`, when pytest collects the tests; the target's
`adapter` is the registered name relay builds a site from, as in `sites.yaml`.
A whole test module, with a fake facility like the template's:

```python
from collections.abc import Iterator
from pathlib import Path

import pytest
from fake_facility import FakeFacility, running  # your fake of the facility's API
from relay_myfacility import MyFacilityAdapter

from clio_relay.adapters import registry
from clio_relay.testing.conformance import (
    AdapterConformance,
    ConformanceTarget,
    use_memory_keyring,
)


@pytest.fixture
def facility_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    use_memory_keyring(monkeypatch, {"myfacility-key": "test-key"})
    registry.register(MyFacilityAdapter)
    with running(FakeFacility(tmp_path / "facility", "test-key")) as url:
        yield url


class TestMyFacility(AdapterConformance):
    adapter = MyFacilityAdapter

    @pytest.fixture
    def target(self, tmp_path: Path, facility_url: str) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="myfacility", config={"url": facility_url}, state_dir=tmp_path
        )
```

`use_memory_keyring` keeps relay's secrets in memory for the test and stores
the ones you give it, which the adapter reads with `self.context.secret`.

It collects tests only for the verbs in your adapter's `implements`:

| Area | What it checks |
| --- | --- |
| Settings | The `Config` schema is an object and unknown settings refuse with `invalid_config` |
| `describe` | Its shape, and that it lists exactly the verbs in `implements` |
| Verbs left out | One test each, `test_<verb>_is_left_out`: the verb refuses with `verb_not_implemented` |
| `exec` | Output is bounded, a time limit holds, no time limit runs to the end, and cancelling the call stops it |
| `files` | Everything in [files](files.md): round trip, `overwrite`, delete, chunked upload and resume, relative paths |
| Jobs | Lifecycle, a failing job, cancel, cancel after the end, inputs by digest, and following a job from a new adapter instance |
| Errors | A missing path is `path_not_found`, and a missing program fails without hanging |
| Policy | Open sites run what the agent asks; guarded sites refuse commands and anything but read-only `exec`, and ask the person before a `files` delete or overwrite |

Each test names its area. For a verb you leave out, the kit collects none of
its tests, only `test_<verb>_is_left_out`. So an adapter with `describe` and
`files` shows the settings, `describe`, `files`, errors and policy tests, plus
`test_run_is_left_out`, `test_exec_is_left_out` and `test_serve_is_left_out`;
`pytest -v` lists exactly what was tested. Add a verb to `implements` and its
tests appear.

Run it against a fake of your facility's API, like the template's
`tests/fake_facility.py`, so it runs anywhere in seconds. Run it against the
real facility too, from its own test module that takes the facility's address
from the environment and fails without it; run that one where you can reach
the facility. `ConformanceTarget` takes the jobs and commands to try
(`ok_job`, `long_job`, `echo`, ...), for a facility whose machines lack `sleep`
or `echo`.

Put the `testing` extra, which brings pytest and pytest-asyncio, in your dev
dependencies, not your package's: `uv add --dev "clio-relay[testing]"`.

## Try it end to end

```powershell
clio-relay adapters
clio-relay site add myfacility-dev --adapter myfacility --config '{"url": "https://..."}' --policy open
clio-relay describe myfacility-dev
clio-relay serve          # then point an agent at it; see the README
```

## Ship it

- **In your own package**, as the template is. You release it on your own
  schedule, and people install it next to relay.
- **In relay itself**, under `src/clio_relay/adapters/<name>/`, with its entry
  point in relay's `pyproject.toml`, its dependencies in an extra, a page in
  `docs/adapters/`, and its kit test under `tests/`. See
  [CONTRIBUTING](../CONTRIBUTING.md).
