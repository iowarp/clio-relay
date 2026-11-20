# Guarded sites

A site is either open or guarded. On an open site the agent runs what it likes:
a command, a script, a container. On a guarded site, such as a national lab
machine where your account is on the line, relay runs only JARVIS packages that
you have reviewed, lets `exec` only read, and asks before it spends your
allocation.

Mark a site guarded when you add it (guarded is the default):

```powershell
clio-relay site add polaris --adapter ssh --config '{ssh_profile: polaris}' --policy guarded --spend always
```

## What the agent can and cannot do

| The agent asks for | On a guarded site |
| --- | --- |
| `run` of a command or script | Refused with `policy_refused`. The refusal says how to go on: write or pick a JARVIS package and run that. Nothing is recorded or submitted. |
| `run` of a JARVIS package you have not reviewed | You get a review question first. Nothing is submitted until you say yes. |
| `run` of a package you reviewed | Runs, after the spend question if the site asks one. |
| `exec` of a read-only command (`squeue`, `ls`, `cat`, `module avail`, ...) | Runs. |
| `exec` of anything else (`rm`, `sh -c`, `scancel`, `jarvis ppl run`, ...) | Refused with `policy_refused` and the list of commands that are allowed. |
| A warm allocation | Held after the spend question; `exec` reaches it with `srun --jobid=<job> --overlap <read-only command>`. |
| `files` list, stat, read, stage, or a write of a new file | Runs. |
| `files` delete, or a write that replaces a file (`overwrite` on a path that exists) | You get a question first. Nothing is deleted or replaced until you say yes. |

## Deleting and replacing files

`files delete` and a `files write` that would replace an existing file wait for
your yes, on every adapter. Writing a new file, inside the work directory or
anywhere else the site lets relay write, does not ask.

- Your yes covers one site, one path, and whether the delete is recursive. It is
  used once: by the delete, or by the last chunk of the upload it approved. The
  next delete or replace asks again.
- A chunked upload asks at its first chunk and goes through on the same yes to its
  last chunk; the chunks in between never ask.
- A client that can show a form asks you inside the call. Any other client gets
  `needs_approval` with the ask and its page; answer it there or with
  `clio-relay asks answer`, and the agent calls `files` again with the same
  arguments.
- If you say no, the call is refused with `policy_refused` and the file stays.

## Running a package

A package is one JARVIS package from a JARVIS repository, such as
`clio_jarvis.gray_scott` from clio-jarvis, or one the agent writes with the
JARVIS package skill. Whoever runs relay stages it once, on the relay machine:

```powershell
python -m clio_relay.packages stage ~/src/clio-jarvis/clio_jarvis clio_jarvis.gray_scott
```

That builds the package's archive, puts it in relay's content store and adds
it to relay's package catalog. It prints the name, the version, the archive's
SHA-256 and where it came from (repository, tag, commit, and whether the
checkout had uncommitted changes). The archive holds the package's own
directory, the repository's shared Python modules, and the `schemas/`
directory next to the repository. Building it twice from the same files gives
the same hash. The version comes from the package's `contract.yaml`, else the
checkout's git tag; give `--version` when it has neither.
`python -m clio_relay.packages list` shows what is staged.

An agent finds the packages with `describe(site=...)`. Each one comes with its
version, SHA-256, summary, parameters (as a JSON Schema, read from the
package's `pkg.py` and `contract.yaml` without running them), outputs, and
whether this site has reviewed it. The agent then calls `run` with a package
instead of an executable, by name:

```json
{
  "site": "polaris",
  "spec": {
    "package": {
      "name": "clio_jarvis.gray_scott",
      "parameters": {"nprocs": 4}
    },
    "attributes": {"queue_name": "debug", "account": "myproject"}
  }
}
```

When only one version of that name is staged, relay pins it and its SHA-256
before anything is checked, so the review below is still of one exact archive.
When several are staged, relay does not guess: `run` refuses with the
candidates (version, SHA-256, when each was staged), and the agent asks you
which one and gives `version`, or `version` and `sha256`.

Before anything is asked or submitted, relay checks the job's layout against
what the site reports in `describe(site=...)` for its partition: the node
count, ranks per node times threads per rank against the physical cores, the
memory per node (up to 90% of a node's), and the walltime against the
partition's limit. A job that does not fit is refused with the limits, so the
agent can size it again. The agent can say why it chose the layout, with up
to four alternatives, in the run's `sizing`; the spend question then shows a
"Why" line and the alternatives.

Relay copies the archive to the site by its hash. A small site script
checks the hash again, unpacks the archive into the job directory, makes a
private JARVIS environment for that job only and adds the package's
repository. Relay writes the pipeline, the package with its parameters, and
on a batch site a `scheduler:` key with the job's nodes, processes per node,
walltime, partition and account. JARVIS then submits it with
`jarvis ppl submit`, so the batch job, its execution record and its hostfile
are JARVIS's own. On a host with no scheduler the script runs the pipeline
with `jarvis ppl run`. Your own JARVIS configuration on the site is not
touched. This is the same on the local, worker, ssh and tapis adapters; the
globus, iri and sky adapters do not run packages, and relay says so before it
asks anything.

A package parameter whose value is the name of one of the run's inputs becomes
that input's absolute path on the site, so `{"stl": "body.stl"}` with an input
named `body.stl` reaches the package as `<job directory>/body.stl`. Neither you
nor the agent spell out where relay puts the job.

`cancel` asks JARVIS to cancel the execution (`jarvis execution cancel`), and a
package started as a service (such as pvserver) is stopped with
`jarvis execution stop`, so JARVIS runs the package's own stop first.

A run can name several packages as one pipeline, in order, under `packages`
instead of `package`. Each one is reviewed and pinned on its own, they share
one allocation, and the spend question lists which nodes each one gets from
its `host_slice` parameter.

When the site lacks software the packages need, or JARVIS is not on the site
at all, relay asks you before it installs anything, and the install runs as
its own job before the run. Nothing is submitted until you say yes. Nothing
is built on a site's login node: JARVIS refuses a build outside an allocation
(exit 4), and relay reports that as a refusal and never retries it.

While a package job runs, and after it ends, `describe(job=...)` shows the
latest step the package reported (`result.progress`) and its outputs, each
with its path on the site and, when the package declares one, its view
descriptor. Relay reads them from the job's JARVIS records with the site's
`files` verb, so this works the same on every adapter that has `files`.

The site script exits with a clear code when something is wrong: 4 when JARVIS
refused to build outside an allocation, 64 for a package
name that is not `<repository>.<package>`, 65 when the archive's hash does not
match the reviewed one, 66 when the archive is missing or has no
`<repository>/<repository>` directory, and 69 when `jarvis` is not on the
site's `PATH`.

## Reviewing a package

The first time a package runs on a guarded site, relay asks you to review it.
The question shows:

- the package's name and version;
- the SHA-256 of the archive;
- where it came from: repository, tag, commit, path;
- every file, with its own hash, size and whether it is executable;
- the parameters of this run.

You answer where you answer any relay question: in your MCP host if it can
ask you, on the approval page relay links, or with `clio-relay asks
answer`. A yes records the review for that site and that hash. The next run of
the same archive needs no review. A changed file changes the hash, so an
edited package is a new package and is reviewed again. A review on one site
does not count on another.

Reviews are kept under relay's state directory, in `guard/reviews/<site>/`,
one file per package name, version and hash, each naming the question it came
from. Deleting a file withdraws that review: the next run of the package asks
again. Relay records a yes only while the run that asked is still waiting to
be submitted, so an old answer never brings a deleted review back.

Relay checks the review again just before it submits, so a package can never
run without one, even if something changes between the question and the
submission.

## Read-only exec

`exec` on a guarded site takes an argument list, never a shell string, and runs
only commands that read:

- files: `ls`, `cat`, `head`, `tail`, `grep`, `wc`, `stat`, `du`, `df`,
  `find` (without `-exec`, `-delete` and other actions), `sha256sum`;
- the scheduler: `squeue`, `sinfo`, `sacct`, `sprio`, `sshare`,
  `scontrol show`, `sacctmgr show`, `qstat`, `pbsnodes` (without the options
  that change a node);
- the machine: `hostname`, `uname`, `nproc`, `whoami`, `id`;
- modules and quotas: `module avail`, `module list`, `module spider`, ...,
  `quota`, `lfs quota`;
- JARVIS reads: `jarvis ppl list`, `ppl status`, `repo list`, `pkg readme`,
  and the like.

A program must be named bare, so that a file called `/tmp/x/ls` cannot pass as
`ls`. Add a site's own read-only tools in `guard.yaml` (below).

## The spend question

Each site's `spend.ask` decides when relay asks before it submits:

- `always`: every job asks;
- `never`: no job asks;
- a number: a job asks when it may use more than that many node-hours.

A new site asks before every job unless you choose otherwise. Set it per site
under Spending on the Compute page (Ask every time, Ask above N node-hours or
Always allow), with `clio-relay site add --spend`, or in `sites.yaml`. The
Compute page's defaults row, or `clio-relay site defaults --spend`, sets what
new sites start with. A site where your allocation is unlimited can be set to
Always allow.

The question shows the nodes, the ranks per node, the walltime, the most the
job may use in node-hours, the queue and the account. Relay never sets a walltime itself. A package job takes
its walltime, queue and process layout from its contract's `resources`, read
with the run's parameters (an omitted parameter takes its default), wherever
the job leaves them unset; the site's defaults fill what is still open. A job
with no walltime has an unknown cost: it always asks (unless the site is
`never`), and the question says the scheduler's default limit applies. On a
cloud site the adapter gives its own price, in US dollars.

A no submits nothing. A question nobody answers leaves the operation waiting,
with nothing submitted, until someone answers it. An approval covers exactly
the job you saw: a changed job needs a new approval.

## Warm allocations

When the agent needs a few quick checks on compute nodes, it can ask for a
warm allocation instead of queuing a job for each:

```json
{"site": "polaris", "spec": {"executable": "clio-relay-warm", "resources": {"node_count": 1}, "attributes": {"queue_name": "debug"}}}
```

It goes through the spend question like any job. Relay then holds the
allocation, and `exec` reaches it with
`srun --jobid=<job id> --overlap <command>`, where the command must itself be
read-only on a guarded site. Only allocations relay holds on that site are
reachable this way.

A warm allocation releases itself once ten minutes have passed since anything
last ran in it, counted from the end of the last command. That is an idle
timer on an allocation, not a limit on anything running: while a step runs in
the allocation it stays, and the holder never stops a running step. Cancel it
sooner with `run` and `cancel`.

## Adding clio-jarvis to a site

The clio-jarvis packages can also live in the site's own JARVIS, at a tagged
release. Relay's `clio_relay.jarvis_site.add_clio_jarvis` clones the tag into
`~/.clio-relay/clio-jarvis/<tag>` on the site, checks the tag's commit when you
give it, and runs `jarvis repo add` unless that repository is already there.
Running it again changes nothing.

## Settings

`guard.yaml` in relay's configuration directory holds per-site settings. It is
optional and read on every check, so an edit takes effect at once.

```yaml
sites:
  polaris:
    jarvis: ~/venvs/jarvis/bin/jarvis   # the jarvis command on the site
    read_only: [myquota]                # more read-only programs, by exact name
    warm_idle_s: 600                    # idle seconds before a warm allocation is released
```

## A limitation

On a relay that runs over stdio on your own machine, the approval page is on
`127.0.0.1`. An agent with a shell on that same machine could reach it. Use
guarded sites from an MCP host that asks you directly, or from a relay served
over HTTPS, where the page needs your operator credential.
