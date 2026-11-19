---
name: use-relay
title: Using relay's compute sites
description: Use this skill to run jobs, commands, file transfers or services on the person's compute sites through relay. It says how to find the sites and how to get each site's own guide.
---

# Using relay's compute sites

Relay reaches the compute sites the person has set up, such as a cluster over
SSH, a Globus Compute endpoint or a Tapis tenant. It has five tools: `describe`,
`run`, `exec`, `files` and `serve`.

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
- `serve` starts a job with ports and returns how to reach it.

A refusal carries a typed reason in `structuredContent.refusal`. Read it and
act on it: a site that needs sign-in, a missing account or an unreachable host
are things to tell the person, not to retry.

## Sites are the person's

Do not add, edit or remove sites. The person manages them on the Compute page
or with `clio-relay site` on the command line.
