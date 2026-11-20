---
name: use-relay
title: Using relay's compute sites
description: Use this skill to run jobs, commands, file transfers or services on the person's compute sites through relay. It says how to find the sites and how to get each site's own guide.
---

# Using relay's compute sites

Relay reaches the compute sites the person has set up, such as a cluster over
SSH, a Globus Compute endpoint or a Tapis tenant. Its five verbs are
`describe`, `run`, `exec`, `files` and `serve`. Beside them, `packages` stages
the JARVIS packages `run` takes, and `shutdown` stops relay's long-lived
processes when the host closes.

## Start with describe

Call `describe` before anything else. Without arguments it lists the sites;
with `site` it describes one site and its routes. Pick a site from that list;
do not guess site names.

## Read the site's guide

A site can carry its own guide: how to submit to that machine, which
partitions and modules to use, where scratch space is. Get it from
`describe(site=...)` before the first run there. When the site has a guide,
the result has a second text block that holds it, and the site's
`skill.text` reads `(below)`. When the `skill` field says relay cannot find
the guide, tell the person; do not invent one.

A site without a guide has no second block. Work from what `describe`
reports.

Relay also serves the guides as MCP resources, `relay://sites/<site>/skill`
for a site's and `relay://skills/<name>` for each skill it knows, for clients
that read resources. They are not files: `files` refuses `relay://` URIs, so
use `describe` instead.

## Run work

- `run` submits a job as a PSI/J JobSpec. When CLIO supports tasks it returns
  a task; otherwise it waits up to `wait_s` and returns the operation, which
  `describe(job=...)` observes.
- `exec` runs one short command on the site's login side and returns bounded
  output.
- `files` lists, reads, writes and stages small files. `stage` returns the
  SHA-256 a JobSpec input names.
- `serve` starts a job with ports and returns how to reach it. Each port in
  the endpoint has an `address` (`host:port`): hand that to the client.

A refusal carries a typed reason in `structuredContent.refusal`. Read it and
act on it: a site that needs sign-in, a missing account or an unreachable host
are things to tell the person, not to retry.

## Stage a package before it runs

`run` takes a JARVIS package by name, `package: {name, parameters}`, and runs
only a package relay has staged. `describe(site=...)` lists the staged ones
under `packages`; `packages(op="list")` lists them too, with where each came
from. When the package you need is not there, stage it with the `packages`
tool. Do not look for relay's files or build an archive by hand.

Stage from one of two sources:

- **Git at a commit.** `packages(op="stage", package="clio_jarvis.xcompact3d")`
  with nothing else stages it from relay's pinned clio-jarvis (the result's
  `default_source` names the URL and commit). For another repository give
  `git` (an https, ssh or file URL) and `commit` (7 to 40 hex digits; never a
  branch or a tag), and `path` when the JARVIS repository is deeper in the
  checkout.
- **A directory on the relay host.** `packages(op="stage", package="...",
  dir="/abs/path")` stages a package from the person's workspace, such as one
  just written with the JARVIS package skill. `dir` is the JARVIS repository
  or the checkout that holds it.

Give `version` only when the package has no `contract.yaml` version and no
git tag. Staging runs nothing and touches no site: on a guarded site the
person still reviews the archive, by its SHA-256, before its first run there.

`packages(op="remove", package="...")` takes a package out of relay's
catalog, every version of it unless `version` or `sha256` narrows it. Use it
when the person resets a demo or wants an older archive gone; staging the
same files again gives the same SHA-256, so an earlier review still holds.
When several archives of one name are staged, `run` refuses and lists them:
ask the person which one and pin its `version` or `sha256`.

## Sites are the person's

Do not add, edit or remove sites. The person manages them on the Compute page
or with `clio-relay site` on the command line.
