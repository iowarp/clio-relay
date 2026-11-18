# TACC Vista

TACC's Vista, reached through TACC's Tapis service. It needs a TACC allocation on
Vista; without one, `describe` reports that the credential check failed.

## Rules on this site

- The site is guarded. `run` accepts only a reviewed clio-jarvis package, given by
  name, version and SHA-256, with its parameters. A free-form executable is refused
  with `policy_refused` before anything reaches Tapis.
- Every job asks the person before it spends allocation hours.
- Set the job's account to the person's allocation, or ask them for it.
- TACC asks for a second factor on login. Tapis's trust manager holds the key, so
  the person only signs in to TACC.

## Running

- Pick a queue from `describe`. Prefer small development queues for first runs.
- The Tapis job UUID is in the task's result. Quote it when you report the run.
- Outputs stay in the job's directory on Vista. Read them with `files`.
- `exec` and `serve` go to the site's `worker` alternative.
