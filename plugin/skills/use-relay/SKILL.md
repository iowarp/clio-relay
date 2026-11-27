---
name: use-relay
title: Using relay's compute sites
description: Use this skill to run jobs, commands, file transfers or services on the person's compute sites through relay. It says how to find the sites and how to get each site's own guide.
---

# Using relay's compute sites

Relay reaches the compute sites the person has set up, such as a cluster over
SSH, a Globus Compute endpoint or a Tapis tenant. Its five verbs are
`describe`, `run`, `exec`, `files` and `serve`. Beside them, `packages` stages
and removes the JARVIS packages `run` takes, `install` installs a package's
software on a site ahead of any run, and `shutdown` stops relay's long-lived
processes when the host closes.

## Reads never ask

Four tools only read: `describe`, `exec_read`, `files_read` and
`packages_read`. They change nothing and never ask the person, so use them
freely, in planning too, and keep `exec`, `files` and `packages` for changes.

- `describe` lists sites, describes one (`site`), observes a job or a
  service (`job`; for a service it gives the endpoint and whether it is
  ready) and lists the staged packages (`packages=true`).
- `packages_read` lists the packages relay has staged, with their versions,
  SHA-256s and where each came from, and relay's package sources. It takes no
  arguments. `packages` has no list: it only stages and removes.
- `exec_read` runs one read-only command, given as an argument list with no
  shell: listings and file reads (`ls`, `cat`, `head`, `tail`, `grep`,
  `stat`, `du`, `df`), the scheduler (`squeue`, `sinfo`, `sacct`,
  `scontrol show`), the machine (`hostname`, `nproc`, `uname`), modules and
  quotas, Spack queries (`spack find`, `spack info`) and the JARVIS reads
  below. Anything else is refused with `policy_refused` and the next step
  `exec`.
- `files_read` lists, stats, reads and checks files (`op` list, stat, read,
  has).

## Start with describe

Call `describe` before anything else. Without arguments it lists the sites;
with `site` it describes one site and its routes. Pick a site from that list;
do not guess site names.

## Read what the site knows about itself

A site's facts are the site's own, not relay's or this skill's. Before you
choose a queue, a node count, storage or an MPI for a run, read them from
`describe(site=...)`:

- `facts.sched` is JARVIS's scheduler report for the site: its provider,
  account and `site_skills`. The partitions it reports (nodes, idle nodes, CPUs
  and memory per node, time limits) are the result's `partitions`, and the MPI
  JARVIS found is its `mpi`.
- `facts.resource_graph` is the site's JARVIS resource graph: where it is, and
  under `net` the data and control networks JARVIS launches MPI on. When `net`
  is empty, `collect` gives the job that fills it in; offer it to the person
  and run it once they agree.
- A site skill is what the person or the site's admins wrote down about the
  machine. `facts.sched.site_skills` lists them; read one with the JARVIS
  command below (`rg skill show NAME +json`).
- The site can also carry the person's own guide in relay. The result then
  has a second text block holding it, and `skill.text` reads `(below)`. When
  the `skill` field says relay cannot find the guide, tell the person. Relay
  also serves guides as MCP resources (`relay://sites/<site>/skill`,
  `relay://skills/<name>`); `files` refuses `relay://` URIs, so use
  `describe`.

To change a site fact, run JARVIS on the site with `exec`, giving
`facts.jarvis.exec` followed by JARVIS's own arguments, such as `rg net set
--data <network> --reason "..." +json` or `rg skill set NAME --text "..."
+json`. Do it only after the person confirms the fact, and say who
confirmed it with `--set_by`. On a guarded site, give `exec` a `plan` that
says what the command changes; relay asks the person before it runs. Do not invent facts about a site, and do not
work them out by hand with `ls`, `module` or interface listings when
JARVIS already reports them.

To read site facts, give `exec_read` `facts.jarvis.read` followed by exactly
one of `sched info +json`, `rg facts show +json`, `rg skill list +json` or `rg
skill show NAME +json`. These run with no shell and no `plan`, even on a
guarded site. Anything else, answers included, goes through `exec` with
`facts.jarvis.exec`.

## Run work

- `run` submits a job as a PSI/J JobSpec. When CLIO supports tasks it returns
  a task; otherwise it waits up to `wait_s` and returns the operation, which
  `describe(job=...)` observes.
- `exec` runs one short command on the site's login side and returns bounded
  output. Use `exec_read` when the command only reads.
- `files` writes, stages and removes small files; read them with
  `files_read`. To move a file that is on this machine, such as a person's STL
  or a result to open locally, use `files(op="upload", local_path=...,
  path=...)` or `files(op="download", path=..., local_path=...)`, at any size.
  Relay streams the bytes itself, checks the SHA-256 on the site and returns
  only metadata. Never move a file as `content_base64` chunks, from a script or
  otherwise, and never read it back to check it: the result's `sha256` and
  `verified` already say it arrived. `local_path` is absolute on the machine
  relay runs on; when relay cannot open it, the refusal says so, and the person
  has to make the path reachable for relay. `stage` returns the
  SHA-256 a JobSpec input names. To hand a staged file to a package, set the
  package parameter to the input's name (such as `body.stl`); relay puts in its
  path on the site. Never write a directory placeholder around it.
- `serve` starts a job that serves ports. Its task completes when the service
  is ready, with the endpoint in the result, while the service keeps running
  until it is stopped. Each port in the endpoint has an `address` (`host:port`):
  hand that to the client. The port is the one the service bound, as JARVIS
  reports it; never choose one yourself. The task completes within seconds of
  the service coming up, while its job still runs: connect to the endpoint or
  open the view right away, in the same turn, since a short job may end soon.
  To serve one package beside a run, such as pvserver beside xcompact3d, give
  both in one `serve` pipeline, with each step's `host_slice` parameter placing
  it on its nodes (pvserver `0:2`, xcompact3d `2:6`, say). JARVIS decides which
  step serves from each package's deployment profile.
- Leave a service package's `linger` at the package's default (pvserver keeps
  serving 600 s after the run ends) unless the person asks for another. The
  live view attaches while the run is going; linger only keeps the service up
  afterwards so the person can still look at the final output.
- Runs write under the site's outputs directory, `describe`'s
  `info.facts.outputs_dir`, which `site_clean` keeps. Leave a package's
  `run_dir` at its default, which lands there. When you set a path for a run's
  results yourself, put it under that directory, never under relay's own
  directory (`~/.clio-relay`): relay refuses a job that names one.
- When another of relay's services still runs on the site, such as a viewer
  kept up for review, the new `run` or `serve` asks the person whether to end
  it when the new job is submitted. Relay asks; never answer for them.
- When relay cannot bring a running service to this machine, the result is an
  error with `result.service_error`, whose `reason` says why, while the job
  runs on. `host_key_unverified` means ssh could not check the compute node's
  host key: tell the person what the message says, and do not retry or work
  around the key check. Stop the job with `cancel` if the person wants that.

A refusal carries a typed reason in `structuredContent.refusal`. Read it and
act on it: a site that needs sign-in, a missing account or an unreachable host
are things to tell the person, not to retry.

When JARVIS refuses a pipeline, `refusal.data.issues` lists its typed issues,
each with `kind`, `field` and `message`, and `refusal.data.kinds` names them.
`configuration_rule_violated`, `host_slice_invalid`, `host_slice_overlap`,
`start_mode_invalid` and `service_port_conflict` name a parameter to change:
change it and call again. `service_after_batch` and `service_blocks_pipeline`
mean the steps are in the wrong order or role for a serve. `package_unloadable`
and `deployment_contract_invalid` are faults in the package itself: tell the
person.

## Ask for enough time

Size a job's time request from what was measured, then pad it generously:

- Projected wall time = measured startup overhead (activating the package's
  environment and any service started beside it becoming ready) + steps x
  seconds per step + output writes, all from a pilot or an earlier run of the
  same build. Take the overhead from the run record's `phase_timings`:
  `before_first_step_seconds` plus each service package's `ready_seconds`.
  Only a record without phase timings falls back to the gap from the job's
  start to its first step; never guess the overhead.
- Seconds per step holds only for the grid and decomposition it was
  measured at (resolution, rank count, process grid, nodes). Never rescale
  it to another grid or rank count: fixed per-step work and communication
  keep a coarser or smaller run from getting proportionally faster, and a
  finer or larger one from staying on the line. If production runs at a
  different grid or decomposition from the pilot, or changes after it,
  re-pilot at production's grid and decomposition, or re-measure from an
  earlier run of exactly that setup, before sizing the request. A scaled
  figure can screen candidates, labelled as unmeasured, but never becomes
  the time asked for.
- Request the projection times a safety coefficient, 1.5 at least and 2 when
  it rests on one short pilot or none, rounded up. Overallocate rather than
  cut it close: a job killed at its limit loses its unfinished work, and
  schedulers such as Slurm charge the time a job used, not the time it asked
  for, so a job that ends cleanly before its limit costs nothing for the
  rest. A longer request can wait a little longer in the queue.
- Say so in the spend approval: the projected time, the coefficient, the
  padded request, and that unused time is not charged when the job ends
  cleanly.
- If the padded request exceeds the partition's time limit (in
  `partitions`), say so and propose options for the person to pick: another
  partition with a longer limit, a shorter run (fewer steps, or a coarser
  grid once a pilot at that grid has measured it), or more nodes when a
  measured run shows they shorten it. Do not trim the coefficient to fit the
  limit.

A package's contract can say how to project its own run; the padding and the
approval above still apply.

## Stage a package before it runs

`run` takes a JARVIS package by name, `package: {name, parameters}`, and runs
only a package relay has staged. `describe(site=...)` lists the staged ones
under `packages`; `packages_read` lists them too, with where each came from.
When the package you need is not there, stage it with the `packages`
tool. Do not look for relay's files or build an archive by hand.

Stage from one of two sources:

- **Relay's package sources.** `packages(op="stage",
  package="clio_jarvis.xcompact3d")` with nothing else stages it from the
  package repositories installed beside relay. This plugin ships clio-jarvis
  at its pin, so its packages need no download, no account and no sign-in.
  The result's `sources` lists the directories relay looked in.
- **A directory on the relay host.** `packages(op="stage", package="...",
  dir="/abs/path")` stages a package from the person's workspace, such as one
  just written with the JARVIS package skill. `dir` is the JARVIS repository
  or the folder that holds it. The archive reaches the site as a job input,
  through the site's own file transfer (scp on an ssh site). Never push a
  person's package to a git repository to get it onto a site.

Give `version` only when the package has no `contract.yaml` version and no
git tag. Staging
runs nothing and touches no site: on a guarded site the person still reviews
the archive, by its SHA-256, before its first run there. A job's packages that
are not reviewed yet come as one review, so one yes covers a pipeline.

`packages(op="remove", package="...")` takes a package out of relay's
catalog, every version of it unless `version` or `sha256` narrows it. Use it
when the person resets a demo or wants an older archive gone; staging the
same files again gives the same SHA-256, so an earlier review still holds.
When several archives of one name are staged, `run` refuses and lists them:
ask the person which one and pin its `version` or `sha256`.

After a plugin update the package sources may build a new archive of a name.
A run, stage or list by name then stages it, and the older archive shows
`superseded_by`. Run the name, not the old `sha256`, unless the person asks for
that archive; a guarded site reviews the new one before it runs.

## Install ahead of a run

When the person wants a package's software on a site before running it ("install
xcompact3d on ares, don't run anything yet"), call `install` with the site and
the same `spec` a run would take, `package: {name, parameters}`, with the
package staged first. It is `run`'s install step on its own, and nothing
launches:

- A site without JARVIS gets relay's jarvis-cd first, after the person says
  yes. `install` does this itself; do not look for another way to place JARVIS.
  On a guarded site the package review says so, and its yes covers jarvis-cd.
- JARVIS checks what the site has where the site chose (below): on the login
  node under JARVIS's limits, or inside a short job on the compute nodes, and
  the task waits while that job queues and runs. Later installs and runs of
  the same packages read the check's record and skip it until the site
  changes. When everything is there, the operation completes with `installed`
  and nothing is built.
- When JARVIS only has a question (which MPI) and the software is already
  there, the person answers the question and nothing else: relay checks again
  with the answer and the operation completes with nothing built.
- When something is missing, the person gets the same asks a run's install
  gets: approve the install and answer any question JARVIS has (below),
  including where it builds. JARVIS then builds it where the site chose and
  checks again, and the operation completes. In a compute job, the approval
  names the job's wall time. On the login node, JARVIS's own install runs
  there under the limits JARVIS sets, as a background task with progress and
  no time limit: a long build is expected, so keep following it and do not
  cancel it yourself. Only the person stops it, with `install(cancel=...)`.

Like `run` it is a task: follow it with the task, or with `describe(job=...)`,
and cancel it with `install(cancel=...)`. To install several packages, such
as everything a plan runs, make one `install` call whose `spec` lists them all
in `packages`, the same steps the later `run` or `serve` takes. The person
then gets one review of the packages, one set of JARVIS questions and one
build, instead of one of each per package. A later `run` of the same
packages finds the software installed and goes straight to its job.

## Questions JARVIS asks

JARVIS asks what it cannot decide alone, and relay turns each question into an
ask for the person: the task turns `input_required` with the question and its
choices. Relay the question as it is and do not answer it yourself. JARVIS
keeps the person's answers for the site, so a later install does not ask again
unless the person wants to change one.

- **Which MPI.** An install with several MPI candidates asks once for the
  site. Installs started together on one site share that question: answer it
  once and every install that asked moves on.
- **Where checks and builds run.** JARVIS asks, once per site, whether its
  software checks (or builds) run on the login node under JARVIS's limits, in
  a compute job, or are asked each time. The question carries JARVIS's
  recommendation and its evidence (the login node's load, the queue wait, the
  cost of each package), and may offer a choice for one package only, such as
  a long build in a compute job while the rest check on the login node. Pass
  it on with that evidence and let the person pick; do not recommend a place
  of your own or set any limit. Builds are offered the same way as checks:
  relay has no default of its own, so a site that has not answered shows
  JARVIS's recommendation. An answer marked for this run only is asked
  again next time.
- **Which Spack.** When JARVIS has no Spack on the site, the install asks which
  of the site's own Spack checkouts to use. Each candidate also has a
  `prepare` command that makes a JARVIS Spack chained to it as an upstream,
  which is better when the person cannot write to the site's own. To prepare,
  run `facts.jarvis.exec` followed by `spack prepare --root
  <facts.jarvis.spack_root> --site_spack <root> +json` with `exec` once the
  person agrees, then call the same `run` again. Exit 3 with
  `ambiguous_compiler` lists the compilers to choose from: ask the person, run
  it again with `--compiler`, and do not pick one yourself.
- **Moving a queued job.** When a job's start is far off and another
  partition would take it now, relay asks the person whether to keep waiting
  or move it, and JARVIS moves the same job on their answer. Do not cancel and
  rerun to switch partitions yourself. When JARVIS refuses the move, the
  message says why.

Leave a job's queue unset (`attributes.queue_name`, or a package's `queue`
parameter) unless the person names one: the site's default applies.

## Site facts JARVIS is unsure of

JARVIS decides deployment details, such as the network MPI launches on, with
fixed rules over the facts it collected, and says nothing when its pick is
sure. When a pick rests on something it cannot settle alone, it raises a
`site_fact_uncertain` issue instead of guessing. Your part is the adaptive
one: find out what this cluster really does, propose a value, and let the
person decide. JARVIS checks and records the answer; you add no network or
site logic of your own.

The issue can reach you in a run's `refusal.data.issues` or `result.issues`,
in `ppl check` output, in a task's question, or in `rg facts show +json`, which
is read-only and lists every open question and recorded answer. Read these
fields:

- `severity`: `block` stops the submit; `advisory` lets the run go ahead
  with JARVIS's pick.
- `details.reasons[]`: what is uncertain, and the `consequence` of each.
- `candidates[]`: the ranked options, and `why` each one ranks there or is
  excluded.
- `details.evidence`: the per-node facts the question rests on.
- `details.pick`: what JARVIS would use, and `consequence_if_wrong`.
- `details.probes[]`: read-only commands, each with the `why`.
- `details.answer`: the accepted values and the command form.
- `details.previous_answer`: when the evidence changed since the person last
  answered, their old answer.

For a blocking issue, work through these steps:

1. **Investigate.** Run the issue's own `details.probes`, read the site's
   skills (`rg skill show NAME +json`), and compare what you find with
   `details.evidence`.
   - Run every probe through `exec_read`; use `exec` only for a probe it
     refuses.
   - Only light, short probes run on the login node. Anything that must see
     compute nodes, or takes longer, runs in a JARVIS job, such as the
     two-node smoke run the probes name (`jarvis ppl submit`).
   - Do not run commands the issue does not offer, and do not reach compute
     nodes any other way.
   - On a guarded site, a probe that needs `exec` takes a `plan` that says
     the command is a read-only probe; relay asks the person before it runs.
2. **Propose.** Tell the person:
   - which value you propose (`accept` for JARVIS's pick, or a value of the
     form `details.answer` lists);
   - the evidence for it;
   - what `consequence_if_wrong` says would happen if the choice is wrong.

   If there is a `previous_answer`, name it and what changed. Then wait for
   the person to confirm. Never answer for them.
3. **Record.** Once they confirm, run `facts.jarvis.exec` followed by `rg facts
   answer <issue_id> --value <value> --reason "<evidence>" --set_by <person>
   --proposed_by <agent> +json`. Use the `issue_id` from the issue. Give
   `exec` a `plan` on a guarded site.
   - `site_fact_answer_stale` means the evidence changed: read the issue
     named by `details.current_issue_id` and propose again.
   - `site_fact_answer_invalid` means JARVIS refused the value: tell the
     person why, and do not work around it.
4. **Continue.** Re-run the check or call the same `run` again. JARVIS now
   uses the confirmed value and stays silent until the evidence changes.

An advisory issue does not hold the run. Mention it to the person with its
consequence. Investigate and propose an answer the same way only when it
matters: when the person asks, or when the run is slow or failing in the way
the issue describes. If the person wants JARVIS to ask again,
`rg facts forget <fact> +json` drops the recorded answer.

## Long runs and failed runs

An install, the queue and the run itself can take hours. When `run` or
`serve` returns a task, say what you are waiting for and end your turn: CLIO
wakes you when the task ends or needs the person, so do not poll `describe` in
a loop. When you are woken, carry on with the next step of the plan the person
approved. A job waiting in the queue says so in the operation's
`result.queue` and message: pass the estimated start on to the person.

When a run fails, read `result.issues`, JARVIS's typed issues, before the
logs. Each names its `kind` and the field it is about. An `mpi_network` issue
means the ranks could not reach each other on the network JARVIS launched
them on: it names the resource graph field and the networks the nodes share.
Do not stop at the diagnosis. Propose the fix to the person (the network the
issue points to, or the resource graph job to collect them again), record it
with `rg net set` once they confirm, and call the same `run` again. Every
later job on the site uses it.

When an install job fails, the run fails `submit_failed` with
`refusal.data.needs: "jarvis ppl install"`. Its message ends with the last part
of the install log, and `refusal.data.install_log` is the whole log on the site:
read it with `files_read(op="read")`. Find the cause, fix it in the place that owns
it (a package parameter, the person's answer to a JARVIS question, the site's
JARVIS Spack through `spack prepare`), and call the same `run` again. Ask the
person only for a real decision, such as a choice between two compilers or a
larger allocation.

When a fact JARVIS holds about the site turns out wrong, propose the change to
the person and record it with JARVIS once they confirm: the network with `rg
net set`, anything else as a site skill with `rg skill set`.

## Stop a service, clean a site

Stop a service or cancel a job with `cancel(operation=...)`. When the person
wants relay's deployment gone from a site, so the next use deploys it again,
call `site_clean(site=...)`: it asks the person first and keeps the site's
Spack store and JARVIS's recorded answers (which MPI, which compiler). It
removes nothing while relay's jobs still run there, as the site's scheduler
reports them, and its refusal names each job and its operation: tell the
person, and cancel them with `cancel(operation=...)` only if they want that,
then clean again. `cancel` also ends a job the scheduler still lists under an
operation that already ended. Pass `forget_decisions=true` only when the
person asks to forget those answers, so the next install asks them again; to
forget one answer, run JARVIS's `ppl install --forget <selector>` with `exec`
instead. When the person wants to start from nothing, so the next install asks
which MPI and where installs run again, that is a request to forget those
answers. What relay's runs wrote, under the outputs directory or elsewhere
under the person's home, is the person's: the clean names those
outputs in its question and keeps them, and the result lists them as
`outputs_kept`. Pass `remove_outputs=true` only when the person asks to delete
them too. Do not look for the `clio-relay` command line to do any of this.

## Sites are the person's

Do not add, edit or remove sites. The person manages them on the Compute page
or with `clio-relay site` on the command line.
