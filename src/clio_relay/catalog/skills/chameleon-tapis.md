# Chameleon through Tapis

A leased Chameleon node with a floating IP, reached through TACC's Tapis service.
The node has no batch scheduler, so Tapis runs each job directly on it as a `FORK`
job.

## Before the first job

- The person signs in to TACC once. Relay sends them to TACC's page with a short
  code; you never see a password or a token. If a call refuses with
  `needs_sign_in`, pass the address on to the person and wait.
- The node must be registered in Tapis once, as the person's own system. Tapis
  logs in from TACC, so the node needs a floating IP and must accept SSH keys.
  Until it is registered, `describe` and `run` refuse with `invalid_config`.

## Running

- `run` takes an executable and arguments. Queues, node counts and durations do
  not apply on a single node.
- The Tapis job UUID is in the task's result. Quote it when you report the run.
- Outputs stay in the job's directory on the node. Read them with `files`, which
  goes through Tapis Files.
- `exec` is a Tapis `FORK` job on the node; `serve` goes to the site's `ssh`
  alternative.
