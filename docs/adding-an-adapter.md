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
clio-relay adapters                       # the adapters installed, and their settings
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
| `describe()` | Reports the site. Required; it must not change anything. |
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
conformance kit against the fake.

```bash
cp -r examples/adapter-template ~/relay-myfacility
cd ~/relay-myfacility
uv run pytest            # 18 passed, before you change anything
```

Then rename `relay_acme`, `AcmeAdapter` and the entry point, and replace each
call to Acme's API with your facility's.

### Settings

Put everything that differs between two sites of your facility in `Config`:
the API's URL, the project or allocation, the work directory. Give each field
a `description`, which the Compute tab shows. Never put a secret in `Config`;
`sites.yaml` is plain text.

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
| `invalid_config` | A site setting is wrong |
| `invalid_spec`, `invalid_arguments` | The job or the call asks for something the facility cannot do |
| `path_not_found`, `path_exists` | Files; see [files](files.md) |
| `input_missing` | A job input names a digest relay does not hold |
| `submit_failed` | The facility rejected a job |

Put what helps in the refusal's data: `Refusal(reason, message, url=...)`.
Anything else that goes wrong is a bug, and relay reports it as one.

### Files

A facility reached over a network subclasses `SiteFiles` and implements six
primitives on absolute paths: `kind`, `describe`, `children`, `get`, `put`
(a whole file) and `remove` (one file, link or empty directory). `SiteFiles`
applies every rule in [files](files.md) on top of them: relative paths,
`overwrite`, chunked uploads and resuming, delete refusals and the content
store. The site needs no append and no recursive delete: chunks collect in a
local staging directory and go up in one `put` when the last one arrives, and
a recursive delete removes one entry at a time. Override `upload` to stream a
large file and `remove_tree` if the site deletes a tree in one call. If the
site caps one upload, set `max_upload_bytes` and say what to use instead in
`upload_hint`. `files()` returns one, built with the site's work directory,
its home directory and `self.context.state_dir / "parts"` for staging. See
`AcmeFiles` in the template.

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
- `cancel(handle)` cancels and returns the status it then observes.
  Cancelling a finished job changes nothing.
- `find(operation_id)` returns the handle of a job relay submitted but never
  recorded, for example because relay stopped mid-call.

A handle is JSON, and relay stores it. A new adapter instance in a new process
must be able to follow and cancel the job from the handle alone: put the
facility's job id in `native_id` and anything else you need in `data`.

### Policy

Each site is `open` or `guarded`. Relay's guard sits in front of every adapter
and enforces the policy, so an adapter does not check it. On a guarded site the
guard turns a reviewed JARVIS package into a plain command job. If your
facility runs packages itself, set `runs_packages = True` and read
`spec.package`.

### Optional hooks

Relay calls these when the adapter defines them:

| Hook | What it is for |
| --- | --- |
| `on_site_added(site)` | Set something up when a site is saved, such as registering a system with the facility |
| `job_name(operation_id)` | The facility's job name, which relay shows |
| `price(spec)` | What a job would cost, for the spend question on guarded sites |
| `system_id()` | The facility's own id for the site, shown in the Compute tab |

### Register it

Name the class in your package's `pyproject.toml`:

```toml
[project.entry-points."clio_relay.adapters"]
myfacility = "relay_myfacility:MyFacilityAdapter"
```

Install your package next to relay (`uv tool install clio-relay --with
relay-myfacility`), and `clio-relay adapters` lists it. In tests, or to try it
without installing, `clio_relay.adapters.registry.register(MyFacilityAdapter)`
does the same in one process.

## Test it with the conformance kit

The kit is one test class, `AdapterConformance` in
`clio_relay.testing.conformance`. Subclass it and say how to build your
adapter:

```python
class TestMyFacility(AdapterConformance):
    @pytest.fixture
    def target(self, tmp_path: Path, facility_url: str) -> ConformanceTarget:
        return ConformanceTarget(
            adapter="myfacility", config={"url": facility_url}, state_dir=tmp_path
        )
```

It checks, for the verbs the adapter implements:

| Area | What it checks |
| --- | --- |
| Settings | The `Config` schema is an object and unknown settings refuse with `invalid_config` |
| `describe` | Its shape, and that it lists exactly the verbs in `implements` |
| Verbs left out | Each one refuses with `verb_not_implemented` |
| `exec` | Output is bounded, a time limit holds, no time limit runs to the end, and cancelling the call stops it |
| `files` | Everything in [files](files.md): round trip, `overwrite`, delete, chunked upload and resume, relative paths |
| Jobs | Lifecycle, a failing job, cancel, cancel after the end, inputs by digest, and following a job from a new adapter instance |
| Errors | A missing path is `path_not_found`, and a missing program fails without hanging |
| Policy | Open sites run what the agent asks; guarded sites refuse commands and anything but read-only `exec` |

Run it against a fake of your facility's API, like the template's
`tests/fake_facility.py`, so it runs anywhere in seconds. Run it against the
real facility too, from a test you skip unless its address is set in the
environment. `ConformanceTarget` takes the jobs and commands to try
(`ok_job`, `long_job`, `echo`, ...), for a facility whose machines lack `sleep`
or `echo`.

Install the `testing` extra for pytest and pytest-asyncio:
`uv add --dev "clio-relay[testing]"`.

## Try it end to end

```powershell
clio-relay adapters
clio-relay site add acme-dev --adapter myfacility --config '{"url": "https://..."}' --policy open
clio-relay describe acme-dev
clio-relay serve          # then point an agent at it; see the README
```

## Ship it

- **In your own package**, as the template is. You release it on your own
  schedule, and people install it next to relay.
- **In relay itself**, under `src/clio_relay/adapters/<name>/`, with its entry
  point in relay's `pyproject.toml`, its dependencies in an extra, a page in
  `docs/adapters/`, and its kit test under `tests/`. See
  [CONTRIBUTING](../CONTRIBUTING.md).
