# ALCF Polaris

Polaris is ALCF's GPU system: 560 nodes, each with one AMD EPYC 7543P, 512 GB of
memory and four NVIDIA A100 GPUs. Jobs run under PBS Pro. Source:
https://docs.alcf.anl.gov/polaris/

## How relay reaches it

- The site uses the `globus` adapter on ALCF's Globus Compute multi-user endpoint
  `9a947ba5-f537-4681-acf3-cc66485aadec`. Nothing is installed on Polaris. The
  person needs a Globus sign-in with the Compute scope and an ALCF account.
- Files go through the `alcf#dtn_home` collection
  (`9032dd3a-e841-4687-a163-2720da731b5b`). Project space on Eagle is
  `alcf#dtn_eagle` (`05d2c76a-e867-4f67-aa57-76edeb0beda0`).
- Alternatives: `iri` at `https://api.alcf.anl.gov`, and `worker` over the
  person's SSH profile. SSH to Polaris needs MFA with MobilePASS+ or a CRYPTOCard.

## Queues

| Queue | Nodes | Longest walltime |
|---|---|---|
| debug | 1 to 2 | 1 hour |
| debug-scaling | 1 to 10 | 1 hour |
| prod | 10 to 496 | 24 hours |
| preemptable | 1 to 10 | 72 hours |
| demand | 1 to 56 | by arrangement |
| capacity | 1 to 4 | 168 hours |

Start in `debug`. Use `prod` only for 10 nodes or more. `preemptable` jobs can be
stopped for urgent work. `demand` needs ALCF's approval.

## What a job needs

- An account: the ALCF project, in `defaults.account` or the job's account. PBS
  refuses a job without `-A`.
- A file systems line. The endpoint adds `#PBS -l filesystems=home` by default.
  A job that reads Eagle needs `filesystems=home:eagle`, which goes in the
  site's `config.user_config.scheduler_options`.
- Walltime and node count from the job spec. The endpoint's keys are `account`,
  `queue`, `walltime`, `nodes_per_block`, `worker_init` and `scheduler_options`.
- The endpoint releases its PBS job after about four minutes with no work
  (`max_idletime` 240), so the next job may wait in the queue again.

## Good habits

- Run a short `describe` first; it reports whether the endpoint is online.
- Keep a debug run under an hour and two nodes.
- Every run on this site goes through the spend gate, since it uses the
  person's allocation.
