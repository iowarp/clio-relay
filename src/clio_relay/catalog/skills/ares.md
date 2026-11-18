# Ares

Ares is the Gnosis Research Center's cluster at Illinois Tech. It runs Slurm on
Ubuntu 22.04. Relay reaches it with the `ssh` adapter, so every call is one
`ssh` or `scp` run from the person's machine with their own key.

## Before the first call

- The person's machine must be on the IIT network or the IIT VPN. Off that
  network, ssh cannot connect and relay says the site is unreachable.
- Ares takes ssh keys only. The `ares` profile in `~/.ssh/config` names the
  host, the user and the key:

  ```
  Host ares
      HostName ares.cs.iit.edu
      User <username>
      IdentityFile <path to the key>
  ```

- Run `describe` on the site first. It reports the partitions, the scheduler
  and the host, so nothing below needs to be guessed.

## Partitions

| Partition | Nodes | Use it for |
| --- | --- | --- |
| `compute` (the default) | 22 | Normal jobs, up to 2 days |
| `debug` | 6 | Short tests, up to 4 days |

Each node has 40 hardware threads (two 10-core Xeon Silver 4114 with
hyperthreading) and 48 GB of memory. `describe` shows the current numbers.

## Running work

- Use `run` for anything heavier than a quick look. The login node is for
  editing, submitting and light work only; compilation and I/O go in a job.
- A job spec with no `queue` lands in `compute`. Set `queue: debug` for a
  quick test.
- Each job gets its own directory under `~/.clio-relay/jobs/<operation id>/`
  on Ares, with `stdout`, `stderr` and the job's outputs. `files` reads it.
- Inputs are staged once by content: a second job with the same input does
  not upload it again.
- Home directories are shared by every node (`/home/<user>`, also
  `/mnt/common/<user>`). Each compute node also has local scratch at
  `/mnt/nvme/<user>`, `/mnt/ssd/<user>` and `/mnt/hdd/<user>`.

## Software

Software is under Lmod (`module avail`, `module load`). Lmod is set up by
login shells, so a job that needs a module should start with
`bash -lc 'module load <name> && <command>'`. `exec` of `module avail` works
as it is: relay runs a program only a login shell defines in one. Spack is
available for custom toolchains.

## Good manners

Ares is shared. Run one heavy job at a time, keep large data off the login
node, and delete scratch you no longer need.

Source: the Ares user guide, https://grc.iit.edu/docs/ares/ares-user-guide/
