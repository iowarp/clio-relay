# Operations

Most people never need this page. An agent reaches a cluster through relay's
five tools, and a `worker` site places and signs in its own worker the first
time it is used (see [the worker adapter](adapters/worker.md)).

This page is for operators who manage a worker cluster by hand: installing the
worker as a lasting service, looking after its queue, testing transports, and
cleaning up sessions. All of these commands are under `clio-relay worker`.
`clio-relay worker --help` lists them, and every command takes `--help`.

Cluster names are your own labels. Nothing in relay treats a particular name,
site or scheduler specially.

## Set up a cluster

```powershell
uv tool install --python 3.12 --no-config clio-relay
clio-relay worker cluster add --name my-cluster --ssh-host my-cluster-login --scheduler-provider slurm
$RelayWheelSha256 = "REPLACE_WITH_SHA256_FROM_THE_RELEASE"
clio-relay worker cluster bootstrap --cluster my-cluster --relay-artifact-sha256 $RelayWheelSha256
clio-relay worker cluster install-endpoint-service --cluster my-cluster --start --enable
```

- `cluster add` writes the cluster's entry in the local registry. `--ssh-host`
  is a `Host` in your ssh config. `--scheduler-provider` is the scheduler the
  worker drives; use `external` when JARVIS or another tool owns scheduling.
  `clio-relay worker cluster list` shows the registry.
- `cluster bootstrap` installs relay, its tools and the worker directories on
  the cluster in one ssh pass. It needs the SHA-256 of the exact wheel installed
  on your desktop (from the release's `SHA256SUMS` or the PyPI file details), so
  a rebuilt wheel with the same version is told apart. To install a local wheel,
  add `--relay-wheel`. Running it again on a healthy cluster changes nothing.
- `cluster install-endpoint-service` installs the worker as a systemd user
  service, so it keeps running after you disconnect and log out.

`clio-relay worker cluster probe --cluster my-cluster` reports the cluster's
runtime health without running anything on it.

### Keep the worker running after logout

The service needs a systemd user manager that outlives your logins. Check and
enable lingering on the cluster:

```bash
loginctl show-user "$USER" -p Linger --value
loginctl enable-linger "$USER"  # once; some sites need an administrator
```

`install-endpoint-service` refuses to install unless lingering is `yes`. If the
site does not allow lingering, `--allow-login-scoped` installs a worker that may
stop at your last logout.

Some clusters have no systemd user manager at all. There this command cannot
install a service; a `worker` site still works, because it places its worker in
the background at sign-in.

### Check and restart the service

```powershell
clio-relay worker cluster endpoint-service-status --cluster my-cluster
clio-relay worker cluster restart-endpoint-service --cluster my-cluster
```

The status command changes nothing. It reports one of `ready`, `recovering`,
`intentional-stop`, `failed`, `inactive-unexpected`, `not-installed` or
`degraded`, and when something needs doing it names the command. A service you
stopped yourself with `systemctl --user stop` stays stopped until you start it.

The restart command checks that the installed unit still matches the cluster's
saved capacity before restarting it. If it does not, reinstall with
`install-endpoint-service`. Restarting keeps the queue and never cancels a
scheduler job.

### Pin the cluster's identity

Pin the physical cluster so a changed host is noticed:

```powershell
clio-relay worker cluster pin-target --cluster my-cluster --target-hostname login-1.example.edu --ssh-host-key-sha256 SHA256:REPLACE_WITH_PIN --scheduler-cluster-name my-cluster
```

Repeat an option to allow several hostnames or host keys, for example during a
key rotation. `--clear` removes the pin.

The first bootstrap of an unpinned cluster pins the hostnames, host-key
fingerprints and site marker it sees, and marks them as trusted on first use,
not verified. Its output lists what it pinned and where each fingerprint came
from, so you can compare them with a trusted source. Later bootstraps never
replace a pin; use `pin-target` for that. The scheduler cluster name is never
pinned automatically.

`clio-relay worker cluster pin-runtime` pins or clears the relay executable and
install receipt the cluster runs.

### Install applications

Bootstrap installs relay, its transport, JARVIS and the configured agent. It
does not install scientific applications, Spack environments, MPI or site
modules. A site that needs an application installed can provide a Python
package that registers a profile in the `clio_relay.application_profiles`
entry-point group, and then run:

```powershell
clio-relay worker cluster install-app --cluster my-cluster --app <profile-name>
```

## Set worker capacity

Capacity is part of the cluster's entry. By default a worker has three slots,
one of them kept for quick queries such as status and progress, so a long job
cannot block them.

```powershell
clio-relay worker cluster install-endpoint-service --cluster my-cluster --concurrency 4 --control-query-concurrency 1 --kind-concurrency jarvis=2 --kind-concurrency remote_agent=2 --start --enable
```

- `--concurrency` is the total number of slots.
- `--control-query-concurrency` is how many of them are kept for queries.
- `--kind-concurrency KIND=LIMIT` caps one kind of job. A cap does not reserve
  slots, and a full kind does not block other kinds.
- `--clear-kind-concurrency` removes every cap.

The values you pass are saved, so a later install without them keeps them.
`clio-relay worker status --cluster my-cluster` shows the saved policy, whether
the running worker agrees with it, and the active leases by kind.

## Look after the queue

The relay queue is separate from the cluster's scheduler. A queued relay job
has not been picked up by a worker yet. A running one may already have
scheduler jobs.

```powershell
clio-relay worker queue list --cluster my-cluster
clio-relay worker queue diagnose <job-id> --cluster my-cluster
clio-relay worker queue stale --cluster my-cluster --older-than 2h
```

`diagnose` explains why one job is not moving: what is ahead of it, its lease,
the worker's capacity, and its latest scheduler state, event and progress.
Reads are bounded, and the output says when a window was cut short; raise
`--limit` or `--scan-limit` to see more.

The queue holds at most 10,000 active jobs, and one session generation at most
10,000 jobs. Past that, new work is refused with `active_job_capacity_reached`
or `owner_session_job_capacity_reached`, while existing work keeps running.

### Clean up stale jobs

```powershell
clio-relay worker queue cleanup-stale --cluster my-cluster --older-than 2h
clio-relay worker queue cleanup-stale --cluster my-cluster --older-than 2h --cancel-queued --no-dry-run
```

Cleanup previews by default. With `--no-dry-run` it recovers expired leases and
stops stale relay tasks. It cancels queued jobs only with `--cancel-queued`, and
it never cancels scheduler jobs. A job that already has scheduler work is not
put back in the queue, so that work is not submitted twice. `--job-id` limits
cleanup to one job.

### Cancel a job

```powershell
clio-relay worker queue cancel <job-id> --cluster my-cluster
clio-relay worker queue cancel <job-id> --cluster my-cluster --cancel-scheduler-job
```

The first form stops the relay job and leaves any scheduler job running. Add
`--cancel-scheduler-job` only when the scheduler job should stop too. The
request is saved with the job, so a worker that restarts still carries it out,
and it is finished only once the scheduler confirms the job is gone.

To read or cancel one scheduler job directly:

```powershell
clio-relay worker scheduler status 12345 --cluster my-cluster
clio-relay worker scheduler cancel 12345 --cluster my-cluster
```

### Retire finished jobs

`queue retention-plan <job-id>` shows what retiring a finished job would remove,
`queue retention-collect <job-id> --execute` does it, and
`queue retention-status <job-id>` shows how far it got. Retiring a job never
cancels scheduler work.

### Check and repair the lease records

```powershell
clio-relay worker queue audit-lease-capacity --cluster my-cluster
clio-relay worker queue repair-lease-indexes --cluster my-cluster
```

Run the audit after storage trouble or offline maintenance. It exits nonzero if
the lease records and their indexes disagree. Repair rebuilds the indexes from
the leases and leaves jobs and scheduler work alone; audit again afterwards.

## Upgrade an older queue

Queues from relay 0.9 need two one-time upgrades. The worker service runs the
first one before it starts:

```powershell
clio-relay worker queue migrate-indexes --all --batch-size 500
clio-relay worker queue migration-status
```

The migration works in small batches and resumes where it stopped. Until it
finishes, the queue refuses new work and reads with an error naming this
command.

Some 0.9 workers stored very large output records. `clio-relay worker init`
reports them and stops. Stop every process that uses the queue, then run:

```powershell
clio-relay worker init --migrate-legacy-output
```

It keeps the original bytes in a private archive, replaces each oversized
record with a bounded one, and can be run again safely if interrupted. Cluster
bootstrap does this itself when it is needed. Neither upgrade deletes jobs,
logs or scheduler work.

## Limit stored job output

A job's stdout and stderr are kept, up to 64 MiB per stream and 128 MiB per
job. Set other limits, in bytes, in the worker's environment:

```bash
export CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM=67108864
export CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB=134217728
```

At the limit the job keeps running. Relay keeps the beginning of the output,
records a `<stream>.truncated` event, and notes the kept and dropped byte counts
in the job's provenance. A retried job must use the limits it started with.

## Test the transports

The worker reaches relay through a rendezvous server (frps) that both sides
dial out to, so the cluster needs no inbound port. Where no rendezvous is
reachable from both sides, a single ssh port forward held for the whole
connection is the fallback. Either way, nothing opens a new ssh connection
after sign-in.

To test the rendezvous path by hand:

```powershell
$env:CLIO_RELAY_FRP_TOKEN = "<shared-frp-token>"
$env:CLIO_RELAY_STCP_SECRET = "<shared-stcp-secret>"
clio-relay worker cluster add --name my-cluster --ssh-host my-cluster-login --frp-server-addr relay.example.edu
clio-relay worker relay-host test-frpc-connection --cluster my-cluster --local-port 8848
clio-relay worker relay-host test-http-transport --cluster my-cluster --local-bind-port 18765
```

Use `--frp-protocol wss` with port 443 for a rendezvous behind Cloudflare, and
`tcp` for a plain public host. `relay-host render-frpc-config`,
`render-frpc-visitor-config` and `render-frps-config` print the frp configs
without starting anything. [Running a rendezvous host](rendezvous-host.md)
covers the server side.

### Run a lasting frpc proxy on the cluster

The tcp transport modes need an frpc proxy that stays up on the cluster.
`install-proxy` renders it, installs it as a systemd user unit, starts it and
checks that it is active, in one ssh pass:

```powershell
clio-relay worker relay-host install-proxy --cluster my-cluster --remote-port 8765
```

- The cluster has to opt in to the shared link secret first. No flag sets it
  yet: set `frp_transport.identity_anchor` to `"preshared_link_secret"` in the
  cluster's entry in `.clio-relay/clusters.json`. Without it, `install-proxy`
  refuses and says so.
- Like `install-endpoint-service`, it needs lingering and refuses before
  writing anything if it is off. `--allow-login-scoped` accepts a proxy that
  stops at logout.
- It writes the frpc config, a 0600 environment file with the real token and
  secret, and the unit. The config holds only placeholders for the secrets. If
  a later step fails, it removes the environment file again.

`proxy-status` shows the unit's state and the last lines of its journal, and
flags a unit that keeps restarting. It is a snapshot: when frps drops after
frpc connected, frpc keeps retrying and the unit can still look active, so read
the journal lines too. `teardown-proxy` removes the unit, its config and its
environment file.

```powershell
clio-relay worker relay-host proxy-status --cluster my-cluster
clio-relay worker relay-host teardown-proxy --cluster my-cluster
```

To test the ssh fallback:

```powershell
clio-relay worker relay-host test-ssh-transport --cluster my-cluster --local-bind-port 18766 --remote-api-port 8766 --session-id relay-ssh-test
```

## Manage sessions

A session is a relay API that a desktop owns on the cluster. It is how a
desktop reaches an installed worker without placing a new one.

```powershell
$env:CLIO_RELAY_API_TOKEN = "<token>"
clio-relay worker session start --cluster my-cluster --session-id desktop-session --remote-api-port 8766 --replace
clio-relay worker session status --cluster my-cluster --session-id desktop-session
clio-relay worker session detach --cluster my-cluster --session-id desktop-session
clio-relay worker session attach --cluster my-cluster
clio-relay worker session teardown --cluster my-cluster --session-id desktop-session
```

- `session start` needs `CLIO_RELAY_API_TOKEN` unless you pass
  `--no-require-token`, which leaves only health checks available. It exits 0
  when the session is ready and 2 when it is still starting; follow it with
  `session start-status` or `session start-watch`. A client that must survive
  losing the starting process runs `session plan-start` first and keeps its
  output.
- `session detach` disconnects the desktop and leaves the session and its jobs
  running.
- `session attach` (or `session reconnect`) finds the last session for the
  cluster, checks it is the same one, and lists its running jobs. It is also
  the one step that dials again after the connection drops. Relay never
  redials on its own, because many clusters need a person for each sign-in.
- `session teardown` stops the session and keeps its jobs. Add `--cancel-jobs`
  to cancel its relay jobs, `--cancel-scheduler-jobs` as well to cancel their
  scheduler jobs, and `--stop-worker` to stop the worker service too. Only jobs
  the session owns are touched, and a scheduler job is cancelled only when its
  ownership is proven. If cancellation takes longer than
  `--relay-cancel-timeout-seconds` (30 by default), teardown stops and can be
  run again with the same flags.

Starting or stopping a session never waits on, or cancels, scheduler work on
its own. A scheduler job may stay pending for days and is still reachable by
its own handle.

## Run the API or an endpoint directly

```powershell
clio-relay worker api start --host 127.0.0.1 --port 8765 --require-token
clio-relay worker endpoint start --role worker --cluster my-cluster
clio-relay worker endpoint status --cluster my-cluster
```

These are what the service and sessions run. Run them by hand only to debug.
`clio-relay worker installation-info` prints the installed relay's identity and
its install receipt.
