# The ssh adapter

The `ssh` adapter reaches a site the way your own `ssh` does. It needs no
daemon, no agent and no open port on the site: relay holds one ssh connection
to the site and runs every call on it, and your keys, your `~/.ssh/config`,
your jump hosts and your ssh agent all apply (see
[how relay connects](#how-relay-connects)).

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
| `remote_dir` | `.clio-relay` | Relay's own directory on the site, under your home unless absolute. `site clean` deletes it, so no run's outputs belong there |
| `outputs_dir` | `clio-relay-runs` | Where runs write: JARVIS's shared directory for each job, and where a case's `run_dir` belongs. Under your home unless absolute; it must be outside `remote_dir`. `site clean` keeps it unless you ask. `describe` shows it as `info.facts.outputs_dir` |
| `scheduler` | `auto` | `auto`, `none`, `slurm`, `pbs`, `lsf` or `cobalt` |
| `jarvis` | empty | Empty runs relay's pinned jarvis-cd. Set only to name another jarvis by path, for [package jobs](#package-jobs) |
| `jarvis_graph` | empty | The site's published JARVIS resource graph, absolute or `~/...`. Empty takes the graph the site's `$JARVIS_RESOURCE_GRAPH` names, else keeps one under `remote_dir`. Set it when the person names another: JARVIS reads it (with the install answers it holds) and never writes it, keeping what it records in its own graph under `remote_dir` (`info.facts.resource_graph.site_home`, jarvis-cd 1.9.16). `describe` reports it as `info.facts.resource_graph.path` with `published: true`, and `site clean` keeps it. `relay` keeps relay's own graph under `remote_dir` even where the site publishes one, so relay offers to collect it; the person's dotfiles stay as they are |
| `install_walltime_s` | unset | The walltime of the job that builds a package's software. Unset, JARVIS sizes it from its estimate of the build, within the partition's limit. Set it only to override JARVIS |
| `interactive_sign_in` | `false` | The site asks for a code on every connection |
| `askpass` | relay's own | The program that answers ssh's prompts |
| `ssh_command`, `scp_command` | `ssh`, `scp` | The ssh relay reads your config with (`ssh -G`). A bare name is looked up on the PATH relay hands the child, with PATHEXT on Windows, so an `ssh.cmd` early on the PATH wins. A path is used as given. A program other than OpenSSH's `ssh` makes relay run it, and `scp_command`, for every call instead of holding the connection |
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
   the logs into the execution directory. `cpu_cores_per_process` counts whole
   cores; above 1 it becomes `cpus_per_task`, which keeps Slurm's meaning of
   hardware threads, so relay multiplies it by the partition's threads per
   core. One core per rank is JARVIS's own default (jarvis-cd 1.9.15), so
   relay sends nothing for it. `exclusive_node_use` becomes `exclusive: true`.
   JARVIS's `+plan` shows what the ranks claim in its `cores` section
   (`jarvis.submit-cores.v1`); relay passes it to the agent unaltered as
   `plan.cores`, and the confirm ask quotes its `meaning`.
2. `relay-jarvis.sh` sets `JARVIS_ROOT` to the site's JARVIS root
   (`<home>/jarvis` in the CLIO site home), computes each package's digest
   there again, refuses one that differs from the reviewed digest, and runs
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
with. Where JARVIS checks and builds on a site is one of those choices
(`install_placement`): JARVIS offers the login host under its limits, a compute
job, or asking each time, with its recommendation, the login host's
measurements and the queue estimate, and per-package options when one package
is costlier than the rest. Relay shows the question and JARVIS's evidence as
they are, records the answer with the site's decisions through `--answers`,
and has no placement policy of its own. Before a Spack build, JARVIS also
searches the site's modules, its Spack and its resource graph for a copy of the
package (`site_copy`). When it finds one, the question is whether to use the
site's copy or build with Spack: each option's meaning is JARVIS's `does` and
`why`, the recommended one is JARVIS's, and JARVIS records the answer for that
package on the site. When it finds none, the install approval says so first.
A chosen site copy that does not load, or a Spack that fails before any
package, is a failure (exit 6, below), not a question. Relay shows each option
as JARVIS wrote it and sends every answer the person gave in the operation, so
JARVIS uses the ones for the questions it still raises.
When the site chose the login host for
checks, the check runs there and relay submits no check job. Where JARVIS
builds is the same choice, and relay declares no default for it, so a site
that has not answered gets JARVIS's own recommendation in the question.
When the site chose a compute job, JARVIS installs the software as its own
job, `jarvis ppl install +submit`, on a compute node, and sizes that job
itself, its walltime included: the build times JARVIS measured at the site,
at most the partition's limit, or the partition's limit when any part of the
build was never timed there. Relay drops the run's walltime from that job,
since a build is not the run, and gives `install_walltime_s` as
`scheduler.install.time` only when the site sets it; then the approval says
how long the build may take. A build longer than the partition's limit takes
more than one job, and the approval says so: Spack keeps every package a job
built, so installing again builds only the rest. When
the site chose the login host, relay starts JARVIS's own
`jarvis ppl install --answers` there, detached, under the operation's
`relay.login-build/` folder. JARVIS sets and applies its limits; relay sets
none and gives the build no time limit, so it runs as a background task with
progress until it ends or you cancel it. When JARVIS refuses a build outside
an allocation (exit 4, `build_outside_allocation`), for example because the
login host had no room for its limits, relay reports it as `policy_refused`
and does not retry it. `describe(job=...)` follows the install log and its progress
for as long as the install takes, then relay submits the run. When something
JARVIS runs fails (a Spack build, a compiler probe, a Spack that breaks on its
own, a site copy that does not load), JARVIS exits 6 with one failure record
per failure (in its install report, and in `install/issues.jsonl` for an
install job): the stage, host, command and exit code, a preview of the output,
`output_path` (the whole output on the site, for `files`) and the logs
involved. Relay asks nothing about it and adds no words. When the login-host
check or submit exits 6, the run fails `submit_failed` with JARVIS's own line for
each failure as the message and the records whole in `failures`. When an install
job exits 6, the operation ends `failed` as a failed run job does: the same
message, the job's `exit_code`, and in its `result` the records whole in
`failures`, every issue in `issues`, `install_log` and `install_job` (the
scheduler's job id). An install job that ends any other way fails the same way,
with the end of what it printed. The agent reads the output, decides what to
change (for example the install's `scheduler.install.exclude`), and the person
confirms the consequential steps. A record can also offer the site's install
settings (jarvis-cd 1.9.14: `install_tmpdir`, the install job's temp dir and
Spack build stage) under `settings`, each with its `issue_id`, JARVIS's question
and what an answer does. Relay passes them whole as `settings` (in the refusal,
or in a failed job's result) and adds JARVIS's question and answer text to the
message. To give one, the
agent runs the same job again with `spec.install_settings`, or installs it again
with `install_settings`, each `{issue_id, value, basis}`; relay takes only an issue_id a failure on the same
site offered, names the value in the install approval, and sends it with every
round's answers (`answered_by: agent`). JARVIS records it for the site
(`decisions.yaml` `site.install_tmpdir`; `ppl install --forget install_tmpdir`
clears it). A guarded site still refuses the same thing as job environment.
An install job that ends any other way fails
with the end of what it printed, as printed.
When a pick rests on a site fact JARVIS cannot settle by itself, such as the
MPI launch network, JARVIS raises `site_fact_uncertain`. A blocking one stops
the check before anything is submitted, and relay refuses the run
`input_missing` with JARVIS's issues as they are (`needs: jarvis rg facts
answer`). The agent investigates with the issue's probes, proposes a value, and
once the person confirms, answers through `facts.jarvis.exec` (`rg facts answer
<issue_id> --value ... --reason ... --set_by <person> --proposed_by <agent>
+json`), then runs again. An advisory one goes ahead, and the operation carries
JARVIS's `site_facts`.
A job's background service can crash while its steps finish, such as a
pvserver beside a solver. JARVIS then completes the job and records the
application's result and each service's result apart, in its
`metadata.outcomes` (`jarvis.execution.outcomes.v1`). The operation carries it
untouched as `result.outcomes`, its text says the application finished but the
service crashed, and the card shows that service's component `failed`. A
service kept as the job's purpose (`after_steps: keep`) still fails the job.
The Spack environment is kept under the CLIO site home, at
`jarvis-cd/pipelines/<digest>/`, where the digest is that of the set of package
digests, so the next run of the same packages reuses it. Two installs into
one environment take turns; the second shows `waiting_env_lock` until the
first is done.
JARVIS records your answers in the site's decisions file,
`jarvis-cd/site/decisions.yaml` in the CLIO site home
(`JARVIS_SPACK_DECISIONS`), and replays them on later installs of any package
while they still match what it finds; it asks again, naming the earlier answer,
when one no longer does. Relay keeps no copy: it asks only what JARVIS leaves
open, and the install approval names the decisions JARVIS applies. Operations
on one site that get the same site-scoped question (the same `issue_id` and
candidates) share one ask, and its answer reaches each of them.

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
A package's Spack repositories are its own repository's, which its `pkg.py`
names in the `install_repos` default of its menu, relative to the package
directory (`../_spack` in clio-jarvis). A repository needs nothing beyond
jarvis-cd's own Python dependencies; relay installs no requirements of its own.

`describe(site=...)` shows JARVIS's view of the scheduler: relay runs `jarvis
sched info +json`, a light read that is safe on a login host, and fills
`info.partitions` (nodes, CPUs and memory per node, the time limit, idle and
allocated counts, and each node's sockets, cores per socket, threads per core
and physical cores) and `info.mpi` (the MPIs an install would offer and whether
the person must choose) from it. `info.facts.sched` holds the provider, the
account, the site skills and JARVIS's `issues`. `info.facts.compute_cores`
gives each partition's compute cores with their `source`: `sinfo`, else the
smallest node in the resource graph's `cpu` section (`resource_graph.cpu`),
else none. `info.facts.cpu` is that graph section as JARVIS wrote it, and
`info.facts.login_cpus` is the login host's count, which is not a compute
fact. `info.facts.jarvis.read` is
`["jarvis"]`: in `exec`, a bare `jarvis` is relay's JARVIS with the site's
record, and its arguments reach JARVIS as they are, never parsed by a shell.
Reads go that way, with no plan even on a guarded site, when they have one of
these exact shapes: `jarvis sched info +json`, `jarvis rg facts show +json`,
`jarvis rg skill list +json` or `jarvis rg skill show <name> +json`.
`info.facts.jarvis.exec` is an `exec` argument prefix (a shell) that runs the
same JARVIS for everything else; append JARVIS's own arguments, such as `rg
facts answer ...` or `spack prepare ...`. On a guarded site each call through
it runs with a `plan` and the person's yes. Before relay has placed JARVIS on the
site, `describe` reads the same partitions from `sinfo` (the query JARVIS's
`sched info` makes), so a clean Slurm site still reports its nodes, idle
nodes, cores and memory per node and time limits; `info.facts.sched.source` is
then `sinfo`, with its `observed_at`. On a guarded site, relay checks a job's
layout against its partition before it asks anything (see [guarded
sites](../guarded-sites.md)). A run's card keeps that partition's current facts
in `result.partition_facts`, read when the card is made, so the job is sized
from them rather than from an earlier `describe`.
The site's cluster facts, such as which network MPI uses, are JARVIS's
resource graph. By default the cluster publishes it: the site's
`$JARVIS_RESOURCE_GRAPH`, read from the call's environment and else from the
person's login shell, names it. Relay reads that graph and never writes it, and
`describe` reports it with `source: site`. The `jarvis_graph` setting, when set,
comes first (`source: setting`). Only when neither names a readable graph does
relay keep one for the site at `jarvis-cd/site/resource_graph.yaml` in the CLIO
site home (`source: relay`). A `jarvis_graph` of `relay` picks that graph even
where the site publishes one, so a person can try relay's collection without
editing their dotfiles: relay's JARVIS calls then run with
`JARVIS_RESOURCE_GRAPH` unset and `JARVIS_SITE_HOME` naming the CLIO site home.
Whichever it is, relay exports it as
`JARVIS_RESOURCE_GRAPH` on every JARVIS call, so it never overrides the site's
own. Only relay's own graph is collected: JARVIS collects its `net` section on
compute nodes (with relay's own
`clio_relay_site.net_graph` package; relay runs JARVIS with `JARVIS_NO_BUILTIN=1`,
so jarvis-cd's builtin packages are never registered), picks the fastest network that reaches every node, and applies it
when it launches MPI, for each MPI flavour. Relay sets no MPI variables itself.
`describe(site=...)` shows `info.facts.resource_graph`: the graph's path and
`source`, the decisions file, JARVIS's net report (`jarvis rg net show`), and,
for relay's own graph only, `collect`, the `exec` that submits the collection
job. A run that fails on the network carries
JARVIS's `mpi_network` issue in `result.issues`, naming the graph field to
change; the person's override is `jarvis rg net set`. `site clean` removes
relay's own graph with the rest of relay's directory, never a published one,
and keeps the decisions file, unless
`--forget-decisions` (`forget_decisions` over MCP) says otherwise.
Relay runs its own pinned jarvis-cd, at `<home>/bin/jarvis` in the CLIO site
home, by absolute path for submit, install, check, execution and stop, always
with `JARVIS_ROOT=<home>/jarvis`. It never runs a `jarvis` found on the site's
`PATH`: an older one rejects the service reports and builds on the login node.
When the root is not set up, or not at relay's pins, a `run`, `serve` or
`install` refuses and names the setup step: `install(site=..., package={name: ...})`,
which asks you before it sets up the site and ends. The refusal says which it
is: a root at other pins reads "JARVIS is on <site> at jarvis-cd X with
clio-jarvis Y; relay updates it to ...", with the pins it holds in `pin` and
relay's in `want`, and the ask is "Update JARVIS on <site>?"; a site with no
root reads "<site> has no JARVIS root that relay set up", and the ask is "Set
up JARVIS on <site>?". The setup installs uv (the
one on `PATH`, else `<home>/bin/uv`, else uv's installer puts it there), then
jarvis-cd at its pinned commit with `uv tool install`, which brings its own
Python, then makes the root without jarvis-cd's builtin repository and clones
clio-jarvis into it at its pinned ref. It needs the site to reach github.com.
`describe(site=...)` shows whether the site is set up, with the root and the
jarvis-cd pin, in its `jarvis` part. A site that really needs another jarvis
names its path in the `jarvis` setting, absolute or from `~/`; a bare name is
refused.

Some things relay refuses rather than working around JARVIS:

- A package job's `directory` and `stdin_path` are refused. Its inputs are in
  the job directory, and a package parameter whose value is an input's name
  becomes that input's absolute path there, so nobody spells out the job
  directory.
- JARVIS has no cleanup command, so the execution directory stays on the site.

Every package job is `jarvis ppl submit`, whatever the scheduler: JARVIS
submits through PSI/J to Slurm, PBS, LSF, Flux or Cobalt, and on a host with
none (`scheduler: none`) to PSI/J's local executor, whose job outlives the
call that started it. A package service stops with `jarvis execution stop`
everywhere. The local, worker and tapis adapters run package jobs the same
way, with the same refusals.

## How relay connects

Relay holds one ssh connection to each site itself, the same way on Windows,
macOS and Linux, and runs every call, copy and forward as a channel on it.
Your ssh still decides where and how to connect: relay reads your
`~/.ssh/config` through `ssh -G <profile>` (host, user, port, keys,
`IdentityAgent`, `ProxyJump`, `ProxyCommand`, known hosts), and your agent
and keys sign in. When the connection drops, a site that signs in with a key
connects again by itself on the next call.

Without a `ProxyCommand` or `ProxyJump`, relay dials through the proxy its
environment names, the way most network clients do: `HTTPS_PROXY`, then
`ALL_PROXY` and `HTTP_PROXY` (HTTP `CONNECT`, or SOCKS5: a `socks5h://` proxy
resolves the site's name, a `socks5://` one is given the address), with
`NO_PROXY` exempting hosts. relay's frp visitor dials through the same code.
That is how relay reaches a site from
inside a sandbox that lets programs out only through its proxy. When the proxy
won't open the tunnel, relay dials directly, as `ssh` itself would. A
connection that fails says which way relay dialed and the system's own error.

A few profiles need the system `ssh` program, and relay runs it, one process
per call, for those sites only:

- `ssh_command` names a program that is not OpenSSH's `ssh`, such as a site's
  own `gsissh`;
- the profile signs with a PKCS#11 token (`PKCS11Provider`);
- the profile signs in with Kerberos: `GSSAPIAuthentication yes` together
  with a Kerberos method first in `PreferredAuthentications`, or a ticket
  cache in `KRB5CCNAME` (the `GSSAPIAuthentication yes` Debian and Ubuntu set
  for every host does not count by itself);
- the key is a FIDO security key (`sk-...`) that no agent holds.

Those sites cannot sign in with a code.

## Sites that ask for a code

Some sites ask for a one-time code or a password on every connection.

- **Without `interactive_sign_in`**, relay never asks. A site that asks is
  refused with `interactive_auth_required`, and the refusal names the
  `worker` adapter.
- **With `interactive_sign_in: true`**, relay signs in once. The site shows
  as needing sign-in; you press "Start sign-in" on relay's page and answer
  each prompt there, a password and then a code, or a key and then a code.
  For a key and then a code, the site's `Host` entry in your ssh config is
  the same as for a key-only site: relay offers the key first, and you are
  asked only for the code.
  Every later call rides that signed-in connection, so you are asked once,
  not on every call, and starting several calls at once still asks once.
  Relay's other processes on this computer that share its state (the page
  server CLIO starts, the agent's own server, a CLI command) use the same
  signed-in connection, as OpenSSH's `ControlMaster` would share a master:
  the process that signed in offers it on a loopback port that only takes a
  key it writes to `<state>/ssh/<site>.json`, readable by your account only.
  When the connection closes (the site drops it, or the process that signed
  in stops), the site refuses with `needs_sign_in` and asks you to sign in
  again. A wrong code is asked again, up to the profile's
  `NumberOfPasswordPrompts`.
- A site in the list above that needs the system `ssh` refuses with
  `interactive_auth_required` and `cause: openssh_only` before anyone is
  asked.

`askpass` replaces the program that answers the prompts, for example with a
program of your own that reads codes from a token. With such a program, relay
signs in by itself and only asks you if the program fails.

## Files

`files` follows [the files contract](../files.md). A relative path is under
`remote_dir`, `~/` is your home on the site, and an absolute path is the
site's own. Each operation is one command on the site, and the bytes travel
on its standard input and output. Staging a job's inputs copies them over
sftp on the same connection.

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

`serve` forwards a local port to the service on the site. When the service
runs on a compute node, the forward reaches it through the login node, and
the node gets the user and key your ssh config gives the login node (read
with `ssh -G`). Relay reads the node's host key from the login node's
`known_hosts` files, so you don't need the node in your own.

On a site that signs in with a key, the forward is its own process (relay's
forwarder, which holds its own connection), not a child of relay's, so it
lives through relay restarting, however the host stops relay: a signal to its
group, a kill of its tree, or on Windows the close of the job object relay
runs in (relay starts it through the WMI service there, outside every job). A
relay that starts while the service is live adopts the forward still holding
the port, or starts it again at once if it has died, before any call. The
forward ends when the job does, or when you stop the service or clean the
site.

On a site that signs in with a code, the forward rides the signed-in
connection inside relay, so it asks for nothing more, and it ends when that
connection does or relay stops. The service keeps running; sign in again and
serve it again to reach it.

## When a command ends

A call ends when its command on the site exits, even when something keeps the
ssh session open after it, such as a background process the command left
holding its output. Relay runs each command so that its exit code is reported
on the session's error stream, with a token unique to the call, and drops that
line from what you see. Once it arrives, ssh gets 5 seconds to end by itself,
then relay stops it and returns the command's own exit code.

For a site that runs the system `ssh` (see [how relay connects](#how-relay-connects)),
Win32-OpenSSH (the `ssh.exe` Windows ships) can hang for good when a session
ends while its standard input is a plain pipe that is still open, as relay's
is for an `exec`: it waits, with no timeout, for a read it failed to cancel,
and never writes the output it holds. So when the program relay runs is
Win32-OpenSSH (its binary carries the `POSIX_FD_STATE` name), relay gives it an
overlapped pipe and declares it in that variable, which Win32-OpenSSH closes
without the wait. Any other ssh, a wrapper such as CLIO's fence (which does the
same for the ssh it starts), or an environment that already sets the variable,
keeps the plain pipe.

## When the connection drops

A dropped link costs only the calls it hit. Relay connects again and repeats
each of them up to `retries` times, waiting 2, 4, 8, 16 and then
30 seconds, and logs each attempt as "retry n of N". The repeat is safe
because each step checks before it acts: a copy skips content already on the
site, a submit first looks for the job it may already have submitted, and
`status` and `cancel` only read or stop. A job keeps running on the site while
the link is down.

Three calls are repeated only when the connection never opened, because the
command may already have run: `exec`, a whole `files` write that may not
overwrite, and `files` delete. Such a call fails with `unreachable` and the
caller decides. Chunks of a `files` write are always repeated. A call whose
command reported its exit is never repeated, whatever ssh said after.

A failed login, an unknown or changed host key and a missing key are not
repeated: trying again would give the same answer. On a site that signs in
with a code, a dropped connection is not opened again by itself: the calls
refuse with `needs_sign_in` until you sign in again.

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
| `connection_lost` | The ssh session ended before the command reported its end, and ssh gave no clearer reason (on Windows its exit is often -1). The command may have run in part, so check before you run a command that changes things again |
| `ssh` | Any other ssh failure. The message carries ssh's last line |
