# The ssh adapter

The `ssh` adapter reaches a site with the `ssh` and `scp` already on your
machine. It needs no daemon, no agent and no open port on the site: every call
is one `ssh` or `scp` run, so your keys, your `~/.ssh/config`, your jump hosts
and your ssh agent all apply.

It serves `describe`, `run` (with `status`, `cancel` and `find` on the jobs it
starts), `exec`, `files` and `serve`.

## Adding a site

A site entry names a host from your ssh configuration:

```yaml
- id: ares
  name: Ares
  adapter: ssh
  config:
    ssh_profile: ares
  policy: open
  spend: {ask: never}
```

`spend: {ask: never}` submits every job without asking, for a site whose
allocation is unlimited. Leave it out and relay asks before every job; the
Compute page's Spending picker changes it.

Check that `ssh ares hostname` works in a terminal first. If it asks you
anything, see [sites that ask for a code](#sites-that-ask-for-a-code).

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `ssh_profile` | (required) | A `Host` from `~/.ssh/config`, or `user@host` |
| `ssh_config` | `~/.ssh/config` | Another ssh_config file to use |
| `ssh_options` | none | Extra `Key=Value` options, passed with `-o` |
| `remote_dir` | `.clio-relay` | Relay's directory on the site, under your home unless absolute |
| `scheduler` | `auto` | `auto`, `none`, `slurm`, `pbs`, `lsf` or `cobalt` |
| `jarvis` | empty | Empty runs relay's pinned jarvis-cd. Set only to name another jarvis by path, for [package jobs](#package-jobs) |
| `jarvis_python` | empty | The Python 3.11 or newer relay installs its jarvis-cd with: a path, `~/...`, a name on the site's `PATH` such as `python3.12`, or `module:<name>` for an environment module. Empty lets relay find one |
| `install_walltime_s` | 14400 | The walltime of the job that builds a package's software. The run's walltime is used when it is longer |
| `interactive_sign_in` | `false` | The site asks for a code on every connection |
| `askpass` | relay's own | The program that answers ssh's prompts |
| `ssh_command`, `scp_command` | `ssh`, `scp` | The programs to run. A bare name is looked up on the PATH relay hands the child, with PATHEXT on Windows, so an `ssh.cmd` early on the PATH wins. A path is used as given |
| `connect_timeout_s` | 30 | ssh's `ConnectTimeout` |
| `command_timeout_s` | 300 | When a `describe`, `status` or `cancel` call counts as stalled. The next poll tries again |
| `retries` | 5 | How many times relay repeats a call or copy whose connection failed or dropped, waiting 2, 4, 8, 16, then 30 seconds. exec, and a write that may not overwrite, are repeated only when the connection never opened |

Jobs and transfers have no time limit. A long upload or a job that runs for
days is allowed to finish.

`exec` has no time limit either unless the call gives one. While a command
prints nothing, the caller hears that it is still running. When the call ends,
times out or is cancelled, or the connection drops, the command and everything
it started are stopped on the site.

`exec` runs its argument list from your home directory, with no shell in
between, so `$HOME`, `*` or `;` in an argument reach the program as they are.
A program that is only defined by your login shell, such as `module` from
Lmod, runs in a login shell (`bash -l`), which gets the same argument list
unchanged. On a guarded site, `exec` runs only the read-only list; anything
else is refused with `policy_refused`.

Relay asks the scheduler for no walltime of its own. A job's walltime is its
`duration`, then the walltime its package's contract gives for the run's
parameters, then the site's `defaults.duration`. With none of these, the batch
script has no time line, and the scheduler's default for the partition or
queue applies.

The spend gate's charge is in node-hours: nodes times walltime. A job with no
walltime has an unknown cost, so its charge has no amount and no node-hours,
and the gate asks for it unless the site's `spend.ask` is `never`.

## How a job runs

1. Relay renders the batch script on your machine with
   [psij-python](https://exaworks.org/psij-python/). The scheduler is the one
   the site has, found by `describe` when `scheduler` is `auto`.
2. Inputs are staged by content into `<remote_dir>/content/cas/` with `scp`.
   An input already on the site is not sent again. If the job deletes an input
   from its directory, for example a file of secrets it reads once, relay
   removes the site's stored copy too when the job ends or is cancelled, unless
   another job still uses it. An input marked `transient` goes when the job
   ends in any case, both the job's copy and the stored one, even if the job
   was cancelled before it started.
3. One `ssh` call writes the script into the job's directory and submits it.
   The directory is `<remote_dir>/jobs/<operation id>/` and holds the script,
   the submit result, `stdout`, `stderr` and the exit code. On Slurm, a job's
   `stdin_path`, `stdout_path` and `stderr_path` may use sbatch's filename
   patterns (`%j`, `%x`, `%u`, `%N`, `%A`, `%a` and `%%`), so
   `hostname-%j.out` becomes `hostname-27874.out`.
4. `status` asks the scheduler through psij's own status command, and reads
   the exit code from the job's directory once the job ends.

Relay keeps no connection open between calls, so it can restart at any time.
After a restart, `find` reads the job's directory and returns the same job, so
a retried `run` never submits a second copy.

`cancel` uses the scheduler's own command (`scancel`, `qdel`). Your command
gets `SIGTERM` first, so a `trap` in it can clean up, and is killed only after
the scheduler's grace period (Slurm's `KillWait`, PBS's `kill_delay`). Some
Slurm sites signal only the batch shell; relay's Slurm script passes the
signal on to your command and waits for it to finish its trap, so it behaves
the same on every site. A command over several lines, such as a multi-line
`bash -c` script, is handled the same way.

On a shared filesystem such as NFS, a file written on one node can stay
invisible on another for a minute or two. Relay lists a directory before it
checks a file in it, which makes the filesystem look again, so job records,
exit codes and outputs show up as soon as they are written.

Schedulers forget finished jobs; Slurm drops them from `squeue` after
`MinJobAge`, often five minutes. When the scheduler no longer lists a job,
`status` reads the exit code the job wrote into its directory. With no exit
code, it asks Slurm accounting (`sacct`) if the site keeps it. With neither,
the job stays open with `native_state` `unknown` and a message saying so,
rather than being called failed. A later poll can still find an exit code, and
`cancel` ends it.

A package job's status also asks `squeue` for the job in any state. A job
cancelled outside relay (`scancel`) ends `cancelled`, and one Slurm no longer
knows, with no exit code and JARVIS unsure of it, ends `failed` with
`native_state` `gone`. `site clean` refreshes the site's open operations
first, so these end on their own; it cancels operations still waiting on the
person and refuses only while a job is really queued or running.

### Hosts with no scheduler

With `scheduler: none` the job runs on the ssh host itself,
detached in its own session (`setsid`, or `nohup` where the host has no
`setsid`). It keeps running after ssh disconnects, and a later `status` reads
its exit code from the job directory. `cancel` stops the whole process group.

`auto` never falls back to that on its own on a batch site's login node. When
it finds no submit command it can use but the host still shows a batch system
(`squeue`, `sinfo`, `qstat` or `flux` on its `PATH` or a login shell's, or a
Slurm, PBS or LSF configuration file), `run` refuses with `invalid_config` and
names what it found. Set the site's `scheduler` to the right one, or to `none`
to run jobs on that host on purpose. A check that gives no scheduler answer at
all is refused too.

### Package jobs

A JARVIS package job is submitted by JARVIS, not by relay, so the safeguards
in `jarvis ppl submit` all apply: the sealed execution directory, its typed
`.jarvis-execution.json` record, the allocation's hostfile, the log
directories made before the submit, and the bound interpreter and
`JARVIS_ROOT`. Relay never runs `sbatch` for a package.

1. Relay writes `relay.pipeline.yaml` into the job directory. Its `scheduler:`
   key carries the run's resources: `nodes`, `ntasks_per_node`, `time` (the
   walltime above), `partition` and `account`, and `output` and `error` only
   when the job sets `stdout_path` or `stderr_path`. Otherwise JARVIS writes
   the logs into the execution directory.
2. `relay-jarvis.sh` checks the archive against the reviewed hash, unpacks it,
   makes a JARVIS root for this job only (`<job dir>/.relay-jarvis`), adds the
   package's repository and runs
   `jarvis ppl submit relay.pipeline.yaml --execution-id <operation id> +json`.
   It keeps JARVIS's handle in `relay.submission.json`.
3. `status` reads `jarvis execution get`, and the outputs come from
   `jarvis execution artifacts` once the record is terminal.
4. `cancel` runs `jarvis execution cancel`, and stopping a package service
   runs `jarvis execution stop`, which lets the job run `jarvis ppl stop`
   before it ends. A job still waiting in the queue is cancelled instead.
   `status` shows what JARVIS recorded about either request. Relay never runs
   `scancel` itself.

A pipeline can have several packages, such as a ParaView server and the
simulation it watches. They share one allocation, and each package's
`host_slice` parameter (for example `"0:12"` and `"12:16"`) says which of its
nodes the package gets. `start_mode` passes through the same way. Relay does
not judge the coupling: before it submits, the site script runs `jarvis ppl
check +json`, which reads each package's deployment profile, its slices and its
configuration rules. When JARVIS refuses (exit 5), nothing is submitted and the
run is refused `invalid_spec` with JARVIS's typed `issues` (`kind`, `field`,
`message`), such as `host_slice_overlap` or `configuration_rule_violated`, so
the agent sees which field to change. When a package is served, relay's
connector (which publishes the port and releases an idle service) runs on the
first node of the service JARVIS's check names, through `srun --overlap` inside
JARVIS's own allocation when that is not the first node.
The submit first checks that the site has the software the packages need.
When it does not, nothing is submitted. Relay asks you to approve the install
(what JARVIS reuses and what it builds), then asks one question for each
choice JARVIS cannot make alone, such as which of two compilers to build
with. JARVIS then installs the software as its own job,
`jarvis ppl install +submit`, on a compute node, and sizes that job itself.
Relay only gives it `install_walltime_s` as `scheduler.install.time` (4 hours
by default, or the run's walltime when longer), since a build often takes
longer than the run, and the approval says how long it may take. Nothing is
built on the login node: when JARVIS refuses a build outside an allocation
(exit 4, `build_outside_allocation`), relay reports it as `policy_refused` and
does not retry it. `describe(job=...)` follows the install log and its progress
for as long as the install takes, then relay submits the run. When Spack
itself fails, JARVIS types the failure and exits 6 (`compiler_unavailable`,
`fetch_failed`, `build_failed`, `variant_conflict`, `no_recipe`,
`concretize_failed` or `install_failed`, each with the host, stage, failed
node, log tail and remedy, in `install/issues.jsonl`). Relay passes it on: a
failure the person answers, such as the compiler to build with, becomes the
install questions again; any other fails the run with JARVIS's `kind`, its
message and remedy, the issues (each with the last 20 lines of the log),
`install_log` (the whole log on the site, for `files`) and `install_job` (the
scheduler's job id). An install job that ends without a typed issue fails
`submit_failed` with the end of the log in its message.
When a pick rests on a site fact JARVIS cannot settle by itself, such as the
MPI launch network, JARVIS raises `site_fact_uncertain`. A blocking one stops
the check before anything is submitted, and relay refuses the run
`input_missing` with JARVIS's issues as they are (`needs: jarvis rg facts
answer`). The agent investigates with the issue's probes, proposes a value, and
once the person confirms, answers through `facts.jarvis.exec` (`rg facts answer
<issue_id> --value ... --reason ... --set_by <person> --proposed_by <agent>
+json`), then runs again. An advisory one goes ahead, and the operation carries
JARVIS's `site_facts`.
The Spack environment is kept under the CLIO site home, at
`jarvis-cd/pipelines/<digest>/`, where the digest is that of the set of package
archives, so the next run of the same packages reuses it. Two installs into
one environment take turns; the second shows `waiting_env_lock` until the
first is done.
JARVIS records your answers in the site's decisions file,
`jarvis-cd/site/decisions.yaml` in the CLIO site home
(`JARVIS_SPACK_DECISIONS`), and replays them on later installs of any package
while they still match what it finds; it asks again, naming the earlier answer,
when one no longer does. Relay keeps no copy: it asks only what JARVIS leaves
open, and the install approval names the decisions JARVIS applies.

While the install job or the run waits in the scheduler's queue, the
operation's `result.queue` and its message carry JARVIS's view of it, the
execution record's `metadata.queue`: why it waits (`reason_kind`), the
scheduler's estimated start, and the other partitions with `idle_nodes` and
`fits`. When the estimate is further off than the site's `defaults.queue_wait`
(one hour unless set) and another partition fits, relay asks the person once
per partition: keep waiting, or move the job there. Moving is `jarvis execution
move --queue <name>`, so the job keeps its id, directory and approvals, and the
operation's `queue_name` follows it; when JARVIS refuses the move (exit 5,
`execution_move_refused`), the refusal carries its typed issue. Relay never
moves a job without that answer, and a wait the job causes itself (a hold), or
one JARVIS says cannot be moved, is not asked about. If the job starts or ends
before the person answers, the ask is withdrawn.
Which Spack JARVIS builds with is JARVIS's question. When the CLIO Spack is
on the site (`<home>/spack` with a `bin/spack`), relay exports it as
`JARVIS_SPACK_ROOT`. When JARVIS finds no Spack it can use, the install exits
3 with a `spack_missing` issue whose candidates are the site's own Spacks
(`site_spacks`, each with its root and version). Relay asks the person to pick
one, like any other JARVIS choice, and JARVIS records the answer for the site.
With no candidate at all, the issue blocks the install and its message says
so. Making a Spack checkout that reuses a site Spack as its upstream is
`jarvis spack prepare --root <home>/spack --site_spack <root> +json`, which
the agent runs through `exec` with `facts.jarvis.exec` (see below), with a plan
on a guarded site. It never builds. When it cannot pick a compiler it exits 3
with an `ambiguous_compiler` issue; answer it with `--answers`. Relay never
builds into the site's own Spack.
A package archive carries what the package needs on the site: the Spack
repositories its repository's `contract.yaml` lists under `install.repos`,
and the Python requirements its `pyproject.toml` names. Relay installs those
requirements into its own jarvis-cd before it registers the repository, and
the review lists them.

`describe(site=...)` shows JARVIS's view of the scheduler: relay runs `jarvis
sched info +json`, a light read that is safe on a login host, and fills
`info.partitions` (nodes, CPUs and memory per node, the time limit, idle and
allocated counts) and `info.mpi` (the MPIs an install would offer and whether
the person must choose) from it. `info.facts.sched` holds the provider, the
account, the site skills and JARVIS's `issues`. `info.facts.jarvis.exec` is an
`exec` argument prefix that runs relay's JARVIS with the site's record; append
JARVIS's own arguments, such as `sched info +json` or `rg skill show <name>
+json`. Before relay has placed JARVIS on the site, `describe` lists only the
queue names. On a guarded site, relay checks a job's layout against its
partition before it asks anything (see [guarded sites](../guarded-sites.md)).
The site's cluster facts, such as which network MPI uses, are JARVIS's
resource graph, kept for the site at `jarvis-cd/site/resource_graph.yaml` in the
CLIO site home and exported as `JARVIS_RESOURCE_GRAPH` on every JARVIS call.
JARVIS collects its `net` section on compute nodes (its `builtin.resource_graph`
package), picks the fastest network that reaches every node, and applies it
when it launches MPI, for each MPI flavour. Relay sets no MPI variables itself.
`describe(site=...)` shows `info.facts.resource_graph`: the graph's path, the
decisions file, JARVIS's net report (`jarvis rg net show`), and `collect`, the
`exec` that submits the collection job. A run that fails on the network carries
JARVIS's `mpi_network` issue in `result.issues`, naming the graph field to
change; the person's override is `jarvis rg net set`. `site clean` removes the
graph and the decisions with the rest of relay's directory.
Relay runs its own pinned jarvis-cd, at `<home>/jarvis-cd/jarvis` in the CLIO
site home, by absolute path for submit, install, check, execution and stop. It
never runs a `jarvis` found on the site's `PATH`: an older one rejects the
service reports and builds on the login node. When relay's is missing or at
another pin, relay asks before it installs the pinned commit there, in a fresh
virtual environment made with a Python 3.11 or newer. Relay looks for one
itself: `python3`, then any `python3.N` on the site's `PATH`, newest first, then
a `python` environment module (`module avail`, newest first). Name
another in `jarvis_python`. When none is new enough, the refusal lists what
relay tried. Package jobs run jarvis-cd with that same Python. The install
needs the site to reach github.com. Later runs on the site use it. A site that really
needs another jarvis names its path in the `jarvis` setting, absolute or from
`~/`; a bare name is refused.

Some things relay refuses rather than working around JARVIS:

- Package jobs need Slurm. On PBS, LSF or Cobalt, `run` is refused with
  `adapter_unavailable`, naming the JARVIS scheduler it needs.
- A package job's `directory` and `stdin_path` are refused. Its inputs are in
  the job directory, and a package parameter whose value is an input's name
  becomes that input's absolute path there, so nobody spells out the job
  directory.
- JARVIS has no cleanup command, so the execution directory stays on the site.

The local and worker adapters run package jobs the same way, with the same
refusals: `jarvis ppl submit` when the site's scheduler is Slurm, and
JARVIS's direct run under the supervisor when it has none.

## Sites that ask for a code

Some sites ask for a one-time code or a password on every connection. Relay
never types into ssh itself.

- **Without `interactive_sign_in`**, relay runs ssh with `BatchMode=yes`. A
  site that asks is refused with `interactive_auth_required`, and the refusal
  names the `worker` adapter, which holds one signed-in connection for you.
- **With `interactive_sign_in: true`**, relay signs in once. The site shows
  as needing sign-in; you press "Start sign-in" on relay's page and answer
  each prompt there. That starts an ssh master connection
  (`ControlMaster`), and every later call reuses it with `BatchMode=yes`, so
  you are asked once, not on every call. When the master closes, the site
  asks you to sign in again. The master closes when relay stops, so none is
  left running after it, and signing in again while one is up reuses it.

`askpass` replaces the program that shows the prompts, for example with a
program of your own that reads codes from a token. With such a program, relay
signs in by itself and only asks you if the program fails.

## Files

`files` follows [the files contract](../files.md). A relative path is under
`remote_dir`, `~/` is your home on the site, and an absolute path is the
site's own. Each operation is one `ssh` call, and the bytes travel on its
standard input and output.

A chunk goes into a temporary file first and joins `<path>.relay-part` only
once all of it has arrived, so a link that drops partway adds nothing. A chunk
sent again after its answer was lost is recognised by the part's size and the
hash of its last bytes, and taken without change; so is the last chunk sent
again after the part was moved into place. `delete` never follows a symlink and
refuses the work directory, your home and `/`, even through a symlink to them.

On Ares, from the login node, a 4 MiB chunk takes about 2 s through the
adapter, of which 0.9 s is opening the ssh connection, and a 4 MiB read about
0.8 s. Carrying a chunk through relay's MCP server adds 0.1 to 0.3 s on an
idle machine; a loaded login node can make that several times longer.

## Serving a port

`serve` forwards a port from the site with `ssh -L`. When the service runs on a
compute node, the forward reaches it through the login node with an
`ssh -W` hop as its `ProxyCommand`. The hop runs the same `ssh` relay found on
your PATH, and the node gets the user and key your ssh config gives the login
node (read with `ssh -G`). Relay reads the node's host key from the login
node's `known_hosts` files, so you don't need the node in your own.

## When the connection drops

Every call is its own ssh run, so a dropped link costs only the call it hit.
Relay repeats that call up to `retries` times, waiting 2, 4, 8, 16 and then
30 seconds, and logs each attempt as "retry n of N". The repeat is safe
because each step checks before it acts: a copy skips content already on the
site, a submit first looks for the job it may already have submitted, and
`status` and `cancel` only read or stop. A job keeps running on the site while
the link is down.

Three calls are repeated only when the connection never opened, because the
command may already have run: `exec`, a whole `files` write that may not
overwrite, and `files` delete. Such a call fails with `unreachable` and the
caller decides. Chunks of a `files` write are always repeated.

A failed login, an unknown or changed host key and a missing key are not
repeated: trying again would give the same answer.

## Troubleshooting

| Refusal | What it means |
| --- | --- |
| `unreachable` | ssh could not connect or log in. The message says why and what to do, and `data.cause` names it (below) |
| `timeout` | A `describe`, `status` or `cancel` call stalled. The next poll tries again |
| `interactive_auth_required` | The site asks for a code. Turn on `interactive_sign_in`, or use the `worker` adapter |
| `needs_sign_in` | The signed-in connection closed. Sign in again on relay's page |
| `adapter_unavailable` | `ssh` or `scp` is not on this machine's PATH; the message names the command |

| `cause` | What to do |
| --- | --- |
| `host_key_unknown` | Connect once with `ssh <host>`, check the fingerprint and accept it. Relay never accepts a host key it cannot check |
| `host_key_changed` | The host's key differs from the one in `known_hosts`. Ask the site whether it changed; if it did, remove the old line with `ssh-keygen -R <host>` |
| `key_refused` | The site took no key relay offered. Add the key to your agent (`ssh-add`) or name it with `IdentityFile`; the message names key files it could not read |
| `unknown_host` | The host name does not resolve. Check the name and your VPN |
| `network` | The connection failed or dropped after the retries. Check the network and VPN |
| `ssh` | Any other ssh failure. The message carries ssh's last line |
