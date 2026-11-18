# ALCF Aurora

Aurora is ALCF's exascale system: 10,624 nodes, each with two Intel Xeon CPU Max
processors and six Intel Data Center GPU Max 1550. Jobs run under PBS Pro.
Source: https://docs.alcf.anl.gov/aurora/

## How relay reaches it

- The site uses the `globus` adapter, but ALCF runs no Globus Compute endpoint on
  Aurora. The person runs their own on a login node.
- Set it up once: on an Aurora login node, install `globus-compute-endpoint`,
  run `globus-compute-endpoint configure clio-relay`, and use ALCF's Aurora
  template from https://github.com/argonne-lcf/alcf-globus-compute
  (`configs/aurora`). Starting it the first time asks for a Globus sign-in.
  Put the endpoint id it prints in the site's `config.endpoint`.
- After that, when the endpoint is offline, relay starts it through the `worker`
  adapter on the login node with `globus-compute-endpoint start clio-relay`.
- Project files are on Flare, through the `alcf#dtn_flare` collection
  (`f39a7a0f-5bfc-46ce-9615-ba9f8592814f`). The entry sets no collection, since
  Flare paths depend on the project; small inputs travel inside the task.

## Queues

| Queue | Nodes | Longest walltime |
|---|---|---|
| debug | 1 to 2 | 1 hour |
| debug-scaling | 2 to 256 | 1 hour |
| prod | 256 to 10,624 | 24 hours |
| capacity | 1 to 16 | 168 hours |
| visualization | 1 to 32 | 8 hours |

Start in `debug`. `prod` starts at 256 nodes.

## What a job needs

- An account: the ALCF project, in `defaults.account` or the job's account.
- A file systems line, such as `#PBS -l filesystems=home:flare`, in the
  endpoint's template.
- The keys relay sends are the ones in the endpoint's own template. Relay's
  defaults are `account`, `queue`, `walltime` and `nodes_per_block`; change
  `config.user_config_keys` if the template uses other names.

## Good habits

- Run `describe` first; it reports whether the endpoint is online.
- Every run on this site goes through the spend gate.
