# NCSA DeltaAI

DeltaAI is NCSA's AI system. Each node has four NVIDIA GH200 superchips, an Arm
Grace CPU with a Hopper GPU in each. Jobs run under Slurm. Source:
https://docs.ncsa.illinois.edu/systems/deltaai/en/latest/

## How relay reaches it

- The site uses the `globus` adapter on NCSA's DeltaAI Globus Compute
  multi-user endpoint `eb28e239-288a-4974-8341-27c49cacb37a`. Nothing is
  installed on DeltaAI. The person needs a Globus sign-in with the Compute scope
  and an ACCESS or NCSA allocation on DeltaAI.
- NCSA does not publish the endpoint's user configuration keys. The entry sends
  the queue as `partition`; check the endpoint's schema on
  https://app.globus.org/compute/endpoints/eb28e239-288a-4974-8341-27c49cacb37a
  and change `config.user_config_keys` if it differs.
- SSH to the login node, `dtai-login.delta.ncsa.illinois.edu`, needs Duo MFA.

## Partitions

| Partition | Use |
|---|---|
| ghx4 | batch jobs, up to 48 hours |
| ghx4-interactive | short interactive work, up to 4 nodes and 2 hours |

## What a job needs

- An account: the DeltaAI allocation, which ends in `-dtai-gh`.
- File systems: `/u` for home, `/projects` for project space, and `/work/hdd`
  and `/work/nvme` for scratch.
- Software built for Arm (aarch64). x86 binaries do not run here.

## Good habits

- Run `describe` first; it reports whether the endpoint is online.
- Every run on this site goes through the spend gate.
