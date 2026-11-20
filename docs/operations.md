# Operations

Most people never need this page. An agent reaches a cluster through relay's
tools, and a `worker` site places and signs in its own worker the first
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

## Stop every relay process

When a host closes, it can ask relay to stop what it left running, without
touching any job:

```bash
clio-relay shutdown --site my-site +json
clio-relay shutdown --all +json
```

The `shutdown` tool on the relay server does the same, with
`{"site": "my-site"}` or `{"all": true}` and an optional `timeout_s` (15 s by
default). Relay stops, for each site in parallel and locally: the worker's
site-serve, the login hop, serve connectors and tunnels, frpc, and a relay
server you detached with `clio-relay serve --http`. It never cancels a JARVIS
job. The JSON result (`relay.shutdown.v1`) lists what stopped, each job still
running (`kind: job`), and each site relay could not reach as a failure. A
detached server that relay cannot see, for example one that runs as another
user or outside the host's sandbox, is asked to exit over HTTP. Each server
writes a private record (mode 0600) under relay's state directory, holding a stop
secret made for that process alone. Shutdown sends that secret as a bearer token
to the server's `POST /relay/stop` route, which refuses any other request. A
process relay still cannot confirm or stop is a failure with the reason, and its
record stays so a later call can try again. The next call to a site signs in and
starts what it needs again.

## Clean a site so the next use deploys again

```bash
clio-relay site clean my-site +json
```

This asks you first, since it deletes files on the site. After a yes it stops
relay's processes there, then removes the worker and its binaries,
site-serve, relay's scripts and job directories, `~/.clio-relay`,
`~/.config/clio-relay` and the JARVIS bootstrap venv under the site home. It
keeps the Spack store (`<site home>/spack`, with every package built there):
rebuilding ParaView takes hours. Pass `--remove-spack` to delete it too, and
`--yes` to approve on the command line. It refuses while a job of the site
still runs. The next call signs in and deploys everything again. Every live
proof starts with it, so deployment is tested each time.

## Stage and remove packages

`run` takes only packages relay has staged, so a package is staged before its
first run. The catalog and the archives live on the relay host, under relay's
state directory, not on any site: `site clean` leaves them alone, and staging
deploys nothing. The `packages` tool does it over MCP:

```json
{"op": "stage", "package": "clio_jarvis.xcompact3d"}
{"op": "stage", "package": "demo.hello", "git": "https://example.org/demo", "commit": "4f2a9c1"}
{"op": "stage", "package": "demo.hello", "dir": "/home/me/src/demo"}
{"op": "list"}
{"op": "remove", "package": "clio_jarvis.xcompact3d"}
```

With only a package name, relay stages it from its pinned clio-jarvis commit
(the result's `default_source`). A git source is fetched on the relay host at
exactly that commit, never a branch, with no prompt for credentials. A `dir`
is a directory on the relay host. Each call returns `relay.packages.v1`: what
it staged or removed, and the catalog after it. Removing takes every version
of the package out of the catalog unless `version` or `sha256` narrows it, and
drops each archive from the content store unless an unfinished operation still
needs it (`archive_kept`). `python -m clio_relay.packages stage` and `list` do
the same from a shell. See [guarded sites](guarded-sites.md#running-a-package)
for how a guarded site reviews a staged package.