---
name: use-relay
title: Using relay's compute sites
description: Use this skill to run jobs, commands, file transfers or services on the person's compute sites through relay. It says how to find the sites and where each site's own skill lives.
---

# Using relay's compute sites

Relay reaches the compute sites the person has set up, such as a cluster over
SSH, a Globus Compute endpoint or a Tapis tenant. It has five tools: `describe`,
`run`, `exec`, `files` and `serve`.

## Start with describe

Call `describe` before anything else. Without arguments it lists the sites;
with `site` it describes one site and its routes. Pick a site from that list;
do not guess site names.

## Read the site's skill

A site can carry its own skill: how to submit to that machine, which
partitions and modules to use, where scratch space is. Relay serves skills as
MCP resources, so read them rather than look for site detail here:

- `relay://sites/<site>/skill` is the skill a site links. `describe` names its
  URI in the site's `skill` field. When that field says relay cannot find the
  skill, tell the person; do not invent one.
- `relay://skills/<name>` is each skill relay knows, the shipped ones and the
  person's own. List resources to see them.

A site without a skill has no skill resource. Work from what `describe`
reports.

## Run work

- `run` submits a job as a PSI/J JobSpec. When CLIO supports tasks it returns
  a task; otherwise it waits up to `wait_s` and returns the operation, which
  `describe(job=...)` observes.
- `exec` runs one short command on the site's login side and returns bounded
  output.
- `files` lists, reads, writes and stages small files. `stage` returns the
  SHA-256 a JobSpec input names.
- `serve` starts a job with ports and returns how to reach it.

A refusal carries a typed reason in `structuredContent.refusal`. Read it and
act on it: a site that needs sign-in, a missing account or an unreachable host
are things to tell the person, not to retry.

## Sites are the person's

Do not add, edit or remove sites. The person manages them on the Compute page
or with `clio-relay site` on the command line.
