# Ares through Tapis

Ares is reached through TACC's Tapis service. Relay submits each job to Tapis, and
Tapis runs it on Ares through Slurm as a Tapis `BATCH` job.

## Before the first job

- The person signs in to TACC once. Relay sends them to TACC's page with a short
  code; you never see a password or a token. If a call refuses with
  `needs_sign_in`, pass the address on to the person and wait.
- The site must be registered in Tapis once, as the person's own system. Until it
  is, `describe` and `run` refuse with `invalid_config` and say so.

## Running

- `run` takes an executable and arguments, as on any open site. Each Slurm
  partition is a Tapis logical queue. Leave the queue unset unless the person
  names one: the site's `defaults.queue` in `describe` applies.
- The Tapis job UUID is in the task's result. Quote it when you report the run.
- Outputs stay in the job's directory on Ares. Read them with `files`, which goes
  through Tapis Files.
- `exec` runs as a Tapis job on the login host (`FORK`) when the system allows
  it, else as a batch job, and `describe` says which. `serve` is not a Tapis
  verb: relay routes it to the site's `ssh` alternative, and `describe` shows
  that route.
