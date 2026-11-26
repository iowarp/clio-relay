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
| `bin/frpc`, `bin/frpc.sha256` | frpc, when the sign-in fetched it or copied in the site's `frpc_binary`, and the sha256 of the fetched download |
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
relay's processes there, then removes this site's own deployment: its
configured `remote_dir`, which holds the worker and its binaries, site-serve,
relay's scripts and job directories, the login hop and the JARVIS bootstrap
venv. Nothing outside it goes, so another relay home on the same account,
with its own `remote_dir`, is left alone. If you moved the login hop with the
site's serve `hop_dir`, clean also removes this site's directory in it. It
keeps the Spack store (`<site home>/spack`, with every package built there):
rebuilding ParaView takes hours. It also keeps JARVIS's recorded install
answers (`<site home>/jarvis-cd/site/decisions.yaml`: which MPI, which
compiler, where checks and builds run), facts about the cluster you confirmed, so the next install does not
ask them again. Pass `--remove-spack` to delete the Spack store too,
`--forget-decisions` to delete the answers, and `--yes` to approve on the
command line. To forget one answer rather than all of them, use JARVIS's
`jarvis ppl install --forget <selector>` on the site. The next call signs in
and deploys everything again. Every live proof starts with it, so deployment is
tested each time.

What relay's runs wrote outside its own directories, such as a case's results
under your home, is yours, so clean keeps it. Runs write under the site's
`outputs_dir` (`~/clio-relay-runs` unless set), outside relay's directories:
JARVIS's shared directory for each job lives there, linked from the job
directory, so a clean removes the link and never the outputs. Relay names a
run's outputs where they live, under the outputs directory, never through that
link. A run or service
whose job names a path inside relay's directories is refused before anything is
asked, with the outputs directory to use instead. The question names the
outputs, taken from the operations' records, and the result lists them as
`outputs_kept`. Each job relay sent to the site, an install's check and build
included, has its directory under the outputs directory even when its record
names no file there, so the question lists those directories too. Pass
`--remove-outputs` (`remove_outputs` over MCP) to delete
them with the rest; the question then says it deletes them.

A clean keeps JARVIS's recorded answers unless you pass `--forget-decisions`, and
its question says so. A take that must start from nothing, asking which MPI and
where installs run again, cleans with `--forget-decisions`.

Clean removes nothing while one of relay's jobs still runs on the site, since
its directory is among what goes. It does not trust relay's records alone: it
asks the site's scheduler (`squeue`, `bjobs`, `flux jobs` or `qstat`) for your
jobs named `relay-...` and the job ids relay's records hold in any state, so a
job whose operation already ended, or that another relay started, still counts,
and the prune on the site checks once more before it deletes. The refusal
names each job, its operation and its state. Cancel them first, then clean
again:

```bash
clio-relay cancel op_3f9c... +json
```

`cancel` does what the `cancel` tool does: it cancels a job from `run` or stops
a service from `serve`, withdraws the questions still pending on it, and prints
the operation as it ends. On an operation that already ended whose job the
scheduler still lists, it cancels that job and notes it in the operation's
history.

Over MCP the `site_clean` tool does the same, keeping the Spack store, the
answers and the runs' outputs; its `forget_decisions` argument deletes the
answers and `remove_outputs` the outputs. It asks
the person on every call, open site or guarded, with a question bound to what
it will remove; once the person says yes, the same call, or the next one with
the same site, cleans. It refuses while one of relay's jobs still runs, the
same way.

## Stage and remove packages

`run` takes only packages relay has staged, so a package is staged before its
first run. The catalog and the archives live on the relay host, under relay's
state directory, not on any site: `site clean` leaves them alone, and staging
deploys nothing. The `packages` tool does it over MCP:

```json
{"op": "stage", "package": "clio_jarvis.xcompact3d"}
{"op": "stage", "package": "demo.hello", "dir": "/home/me/src/demo"}
{"op": "remove", "package": "clio_jarvis.xcompact3d"}
```

The `packages_read` tool lists what is staged. It takes no arguments and only
reads, so a host that allows only reads (a plan mode) can call it.

With only a package name, relay stages it from its package sources: the
directories `CLIO_RELAY_PACKAGE_SOURCES` names (`os.pathsep` apart), listed in
the result's `sources`. Each is a JARVIS repository, a checkout that holds one,
or a folder of such checkouts. The host fills them when it installs relay, so
staging fetches nothing and runs no git, and it works for anyone, with no
account and no write access to any repository. The CLIO plugin ships a copy of
clio-jarvis at its pinned commit and points `CLIO_RELAY_PACKAGE_SOURCES` at it.
A copy says where it came from in a `.clio-relay-source.json`, which goes into
the archive; a git checkout says it through git, read only. With no source that
holds the package, the refusal names the repository and the directories relay
looked in. A `dir` is a directory on the relay host, such as a package the
person wrote: it is never pushed anywhere, and its archive reaches a site as a
job input through the site adapter's transfer (scp for ssh). Each call returns
`relay.packages.v1`: what
it staged or removed, and the catalog after it. Removing takes every version
of the package out of the catalog unless `version` or `sha256` narrows it, and
drops each archive from the content store unless an unfinished operation still
needs it (`archive_kept`). `python -m clio_relay.packages stage` and `list` do
the same from a shell. See [guarded sites](guarded-sites.md#running-a-package)
for how a guarded site reviews a staged package.

When the package sources change, for example after a plugin update ships a
newer clio-jarvis, a name follows them. A stage or run by name, `list` and
`describe` build the name's archive from the current sources. If its SHA-256
is new, relay stages it and marks the older archives of that name with
`superseded_by`. A name then never resolves to a superseded archive, so a run
pins the new one and a guarded site asks its review again. A superseded archive
stays in the catalog: an operation that pinned it keeps working, and a run that
gives its `sha256` still uses it. An archive staged from a `dir` is never
superseded, and `site clean` leaves the catalog alone.

The archive of the same files has the same SHA-256 on any relay host, from any
checkout and under any Python: members are sorted, with no times or owners, text
files keep LF line ends (a Windows checkout's CRLF is packed as LF), and a file's
mode is 0755 when it starts with `#!` and 0644 otherwise. The gzip holds stored
(uncompressed) deflate blocks, because compressed bytes differ between zlib builds
(CPython 3.14 on Windows links zlib-ng), so an archive is about the size of its
files.