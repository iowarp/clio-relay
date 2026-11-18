# Operations

Most people never need this page. An agent reaches a cluster through relay's
five tools, and a `worker` site places and signs in its own worker the first
time it is used (see [the worker adapter](adapters/worker.md)). There are no
commands to install, start or stop the worker by hand.

This page is for operators who want to know what placement leaves on a cluster
and how to look after it.

## What the worker leaves on the cluster

Everything lives in the worker directory, `remote_dir` in the site's settings
(by default `~/.local/share/clio-relay/worker/<site>`):

| Path | What it is |
| --- | --- |
| `bin/clio-relay`, `tools/` | The relay install `uv tool install` made |
| `bin/frpc` | frpc, when relay uploaded its own |
| `dist/` | The wheel relay installed from, when the site sets `relay_wheel` |
| `run/settings.json`, `run/registry.json`, `run/frp.env` | The worker's ids, tokens and rendezvous settings (0600) |
| `run/site.pid`, `run/ready.json` | The running worker, and its ready report |
| `run/site.log`, `run/frpc.log` | The worker's and frpc's output |
| `adapter/<namespace>/` | Each relay's jobs and staged content, apart from any other relay's |

The secrets in `run/` are new at every sign-in. Nothing else on the cluster
holds them.

## Check that the worker is up

On the cluster:

```bash
D=~/.local/share/clio-relay/worker/my-site
kill -0 "$(cat "$D/run/site.pid")" && cat "$D/run/ready.json"
tail -n 40 "$D/run/site.log" "$D/run/frpc.log"
```

From relay, a site that was never signed in refuses calls with
`needs_sign_in`. While its worker is not answering, calls are refused with
`unreachable`, and signing in again places a new worker.

## Restart or replace the worker

Sign in to the site again. Each sign-in stops the worker it finds in the
directory, frpc included, installs the current relay and starts a new worker
generation with new secrets. Jobs and content under `adapter/` stay.

A worker that an older relay placed is refused with a sign-in rather than used,
so upgrading relay on your machine upgrades the worker at the next sign-in.

## Keep the worker running

The worker starts in the background at sign-in and needs no systemd user
manager, so it runs on clusters that have none. It stops when the login node
reboots, or when the site kills user processes at logout. Sign in again after
either.

frpc reconnects on its own while the worker runs. If the rendezvous server was
down, the link comes back once it is up again.

## Stop the worker and clean up

To stop the worker on the cluster:

```bash
D=~/.local/share/clio-relay/worker/my-site
kill "$(cat "$D/run/site.pid")"
```

frpc stops with it. To remove everything relay placed, delete the worker
directory once no jobs you need remain under `adapter/`. Remove the site with
`clio-relay site remove my-site` so relay stops offering it.