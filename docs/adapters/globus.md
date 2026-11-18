# The globus adapter

The `globus` adapter runs jobs through [Globus Compute](https://www.globus.org/compute)
and moves files through Globus Transfer. A Compute endpoint only makes outbound
connections, so the site needs no open port and relay installs nothing there.

It serves `describe`, `run` (with `status`, `cancel` and `find` on the jobs it
starts) and `files`.

Install it with the `globus` extra:

```
uv tool install 'clio-relay[globus]'
```

## Adding a site

Where the facility runs a multi-user endpoint, the site entry names it. The
catalog has Polaris and DeltaAI set up this way:

```yaml
- id: polaris
  name: ALCF Polaris
  adapter: globus
  identity: globus
  config:
    endpoint: 9a947ba5-f537-4681-acf3-cc66485aadec
    scheduler: pbs
    queues: [debug, prod]
    collection: 9032dd3a-e841-4687-a163-2720da731b5b
  defaults: {account: <your project>, queue: debug}
```

Where you run your own endpoint, as on ALCF Aurora, configure it once on a
login node, put its id in `endpoint`, and add `start` so relay can bring it back
when it goes offline:

```yaml
  config:
    endpoint: <the id your endpoint printed>
    start:
      adapter: worker
      config: {ssh_profile: aurora}
      command: [globus-compute-endpoint, start, clio-relay]
```

The start command runs through that adapter's `exec` with no time limit. Relay
logs each quiet stretch and stops only if the call is cancelled. Set
`start.timeout_s` if you want a limit.

## Signing in

The adapter uses relay's `globus` identity. It needs the Globus Compute scope
for jobs and the Globus Transfer scope for files. When there is no token, or the
token has expired, the site shows as needing sign-in and calls are refused with
`needs_sign_in`. Sign in from relay's page; the adapter never starts a sign-in
itself.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `endpoint` | (required) | The Globus Compute endpoint id |
| `scheduler` | none | `pbs`, `slurm` or `none`, shown by `describe` |
| `queues` | none | The queues the site offers, shown by `describe` |
| `user_config` | none | Fixed values for the endpoint's template, such as `worker_init` |
| `user_config_keys` | see below | Which template key takes each job setting |
| `walltime_format` | `hh:mm:ss` | `hh:mm:ss`, `minutes` or `seconds`, as the template expects |
| `job_root` | `~/.clio-relay/jobs` | Where job directories go on the site |
| `cancel_poll_s` | 5 | How often a running job checks for a cancel |
| `inline_input_limit` | 6 MiB | Inputs up to this size travel inside the task |
| `collection` | none | The site's Globus collection, for `files` and large inputs |
| `collection_root` | `/~/` | Where relative paths start on that collection |
| `content_dir` | `.clio-relay/content` | Where staged inputs go on the collection |
| `local_collection`, `local_path`, `local_dir` | none | A collection on relay's own machine, such as Globus Connect Personal, for reading and writing files |
| `start` | none | How relay starts your own endpoint when it is offline |

Every endpoint template names its settings differently. `user_config_keys` maps
the job's account, queue, walltime, node count and reservation onto them. The
defaults, `account`, `queue`, `walltime` and `nodes_per_block`, match ALCF's
endpoints. Set a key to `null` when the template has no such setting. A job with
no walltime sends none, and the endpoint's own default applies.

## How a job runs

1. Relay submits one Compute task, a small self-contained function that uses only
   the Python standard library. The job's account, queue, walltime and nodes go
   in the endpoint's user configuration, so the endpoint asks the scheduler for
   that allocation.
2. The task writes the job's inputs and a `relay-state.json` record into
   `<job_root>/<operation id>/`, runs the command in its own process group, and
   writes `stdout`, `stderr` and the exit code there.
3. `status` asks Compute for the task. After a restart, `find` uses the record
   relay keeps, so a retried `run` never submits a second copy.

Globus Compute cannot cancel a task once it is submitted. `cancel` sends a
second task that writes a `relay-cancel` marker; the running job sees it and
stops its process group. On a busy endpoint that second task may wait behind
the job before it runs.

A multi-user endpoint gives its batch job back after a few idle minutes (ALCF's
`max_idletime` is 240 seconds), so a job sent later may wait in the queue again.

## Files

`files` lists, reads and writes through Globus Transfer. Listing works with only
the site's `collection`. Reading and writing need a collection on relay's
machine too, since Transfer moves data between two collections. A transfer that
keeps faulting is cancelled and refused; one that is making progress has no
time limit.
