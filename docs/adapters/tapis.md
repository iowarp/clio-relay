# The tapis adapter

The `tapis` adapter runs jobs through [Tapis](https://tapis.readthedocs.io/),
TACC's API for systems, apps, jobs and files. Relay talks to the Tapis
tenant over HTTPS, and Tapis logs in to the host and runs the job. Your
machine needs no SSH access to the host while a job runs.

It serves `describe`, `run` (with `status`, `cancel` and `find` on the jobs it
starts), `exec` and `files`. It does not serve `serve`: a Tapis job's port is
reachable only on the system's network, and Tapis offers no tunnel to a job's
node. A site that needs `serve` names an `ssh` or `worker` alternative.
`describe` shows which adapter serves each verb; with no alternative, the
`serve` route has no adapter and `not_offered` gives that reason, and so does
the refusal.

The host needs bash 4.4 or later, which `exec` uses to bound its output.
`describe` says so in `info.facts.needs`.

The catalog has four Tapis sites:

| Site | Host | How Tapis logs in | Jobs |
| --- | --- | --- | --- |
| `chameleon-tapis` | A Chameleon node with a floating IP | A key pair relay makes | `FORK`, open |
| `ares-tapis` | IIT's Ares | A key pair relay makes | `BATCH` on Slurm, open |
| `vista` | TACC Vista | TACC's trust manager | `BATCH`, guarded |
| `stampede3` | TACC Stampede3 | TACC's trust manager | `BATCH`, guarded |

Ares takes SSH only from inside IIT's network or its VPN, so Tapis at TACC
cannot reach it. `ares-tapis` cannot register until IIT opens SSH to TACC.

## Signing in to TACC

Tapis needs you signed in to TACC. Relay uses the OAuth device code sign-in, so
you sign in on TACC's own page and never type a password into relay.

1. The first call that needs Tapis asks you to sign in. An MCP client that
   supports URL elicitation shows TACC's page and a short code. A client
   without it gets a `needs_sign_in` refusal carrying the same page.
2. On TACC's page, enter the code and sign in with your TACC account and its
   second factor.
3. Relay sees the sign-in within a few seconds and carries on. A code lasts 10
   minutes. If it runs out first, relay gets a new one and shows it on the same
   page, with no limit on how long you take.

Relay keeps the tokens in your operating system's keyring under the service
name `clio-relay`. On a machine with no usable keyring, such as a headless
server, relay keeps them in `credentials.json` in its configuration directory
instead, readable only by you (mode 0600). `describe` shows which one under
`credentials`. The tokens are never shown to the model or logged. The short code
is meant for you to read and is not a secret.

TACC's access token lasts 30 days. Relay refreshes it on its own when it has
the OAuth client's key. Without the key, or when TACC refuses the refresh,
relay signs you out and the next call asks you to sign in again.

You can also sign in from a terminal:

```
python -m clio_relay.adapters.tapis sign-in --site ares-tapis
python -m clio_relay.adapters.tapis status --site ares-tapis
python -m clio_relay.adapters.tapis lifetimes --site ares-tapis
python -m clio_relay.adapters.tapis sign-out --site ares-tapis
```

`lifetimes` prints when each stored token expires, without the tokens.

### The OAuth client

The device code sign-in needs an OAuth client in the `tacc` tenant. Relay ships
none yet, so make your own, once. Any TACC account can. Run this on your own
machine; curl asks for your TACC password, which goes only to TACC:

```
curl -u <your TACC username> -H 'Content-type: application/json' \
  -d '{"description": "clio-relay"}' https://tacc.tapis.io/v3/oauth2/clients
```

TACC answers with a `client_id` and a `client_key`. Put both in the environment
relay runs in. Under CLIO, that is the environment CLIO starts in, because
relay's server inherits it. Never paste them into a chat.

| Variable | What it is |
| --- | --- |
| `CLIO_RELAY_TACC_CLIENT_ID` | The client's id |
| `CLIO_RELAY_TACC_CLIENT_KEY` | The client's key. Needed only for refresh |
| `CLIO_RELAY_TACC_BASE_URL`, `CLIO_RELAY_TACC_TENANT_ID` | Another Tapis tenant |

A site's `client_id` setting takes precedence over the environment. Without a
client, a Tapis site shows "relay has no OAuth client"; with an id TACC does
not know, it shows "TACC has no OAuth client".

## Adding a site

A site entry names the Tapis tenant and, for a host you reach yourself, the
`ssh` alternative relay uses to register it:

```yaml
- id: ares-tapis
  name: Ares (through Tapis)
  adapter: tapis
  identity: tacc
  config:
    base_url: https://tacc.tapis.io
    tenant_id: tacc
    authn: pki
  alternatives:
    ssh:
      ssh_profile: ares
  policy: open
```

Relay asks before every job on this site until you set its Spending to
Always allow (`spend: {ask: never}`) on the Compute page.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `base_url` | `https://tacc.tapis.io` | The Tapis tenant's address |
| `tenant_id` | `tacc` | The tenant |
| `authn` | `pki` | `pki`: relay makes a key pair for a host you reach over SSH. `tms`: TACC's trust manager makes and holds the key |
| `system_id` | `clio-relay.<username>.<site id>` | The Tapis system. Set it to use a system someone else registered |
| `work_dir` | the system's job working directory | Relay's directory on the system, as a Tapis path |
| `client_id` | the environment | Relay's OAuth client, when not the shipped one |

An old `launcher` setting is refused with `invalid_config` naming the field;
remove it from the site's config.

## Registering a host

Tapis runs jobs on a *system*: a host it can log in to. Relay registers one
system for you per site, once. It does this when you add the site, add it from
the catalog, edit it or switch to it, and reports what it did. If the
credential already works, it leaves everything alone, so doing it again is
safe. If you are not signed in yet, the add reports `needs_sign_in` and keeps
the site. After you sign in, the first call that needs the system registers
it, using the way in the add saved, so there is nothing to add again. You can
also register by hand:

```
python -m clio_relay.adapters.tapis register-host --site ares-tapis
python -m clio_relay.adapters.tapis describe --site ares-tapis
python -m clio_relay.adapters.tapis unregister-host --site ares-tapis
```

Until a site is registered, `describe` and `run` refuse with `invalid_config`
and say so.

### Your own host (`authn: pki`)

For a host you already reach over SSH, such as a Chameleon node or Ares:

1. Relay reads your login name, home directory and, on a Slurm host, the
   partitions, through the site's `ssh` alternative.
2. It creates the Tapis system `clio-relay.<username>.<site id>`, with its job
   working directory at `~/.clio-relay/tapis`. Each Slurm partition becomes a
   Tapis logical queue, with the partition's time limit, and jobs run as
   `BATCH` jobs. A host with no scheduler runs `FORK` jobs.
3. It makes a new RSA key pair, adds the public key to your
   `~/.ssh/authorized_keys` with the comment `clio-relay-tapis:<system id>`,
   and gives the pair to Tapis as your credential for the system.
4. It deletes the private key from your machine. Only Tapis keeps it.
5. Tapis checks that it can log in. If it cannot, relay takes the key line
   back out and reports what Tapis said.

The host must accept SSH logins from TACC with a key. A host that asks for a
second factor on every login, or that takes SSH only from a campus network or
VPN (Ares), cannot be registered this way.

`unregister-host` removes the credential, the system relay made, and the key
line with relay's comment. It never removes a system relay did not make.

### TACC hosts (`authn: tms`)

TACC's own hosts ask for a second factor, so relay makes no key. It asks Tapis
to create the credential with TACC's trust manager (`createTmsKeys`), which
holds the key on TACC's side. The systems are TACC's own; relay registers only
your credential and never creates or deletes the system. You need an
allocation on the host.

Relay does not guess which TACC system to use. The catalog's `vista` and
`stampede3` name the host but no system, so adding one refuses with
`invalid_config` and lists the Tapis systems on that host in `candidates`. Set
the one you want, and the site registers:

```
clio-relay site edit vista '{config: {system_id: <a candidate>}}'
```

If the system's job working directory is not a plain path (for example it is
worked out on the host from `$WORK`), set `work_dir` to a plain path under
your work directory as well.

## How a job runs

1. Relay registers its launcher once per system as a Tapis app named after the
   system. The app is a ZIP-runtime app whose archive is a small gzip tar
   holding one shell script. Relay uploads the archive to `<work dir>/launcher/`
   through Tapis Files. The app version holds the script's digest, so a new
   launcher is a new app version.
2. Inputs are staged by content into `<work dir>/.relay-content/` through Tapis
   Files.
   An input already there is not sent again. The job lists each input as a
   `tapislocal` file input, so Tapis does not copy it.
3. Relay submits the job with `submitJob`. The job's description carries the
   relay operation id, and the app arguments carry the job spec as one base64
   argument, so nothing is quoted through a shell.
4. The launcher links the inputs into the job's directory, runs the
   executable and writes the exit code to `relay.exitcode`.
5. `status` reads the Tapis job status, and the exit code once the job ends.
   `cancel` calls `cancelJob`. Cancelling a job that already ended reports how
   it ended.

## exec

`exec` is a launcher job too, so it means the same here as on the other
adapters and keeps their read-only rules. Relay runs it as a `FORK` job, on the
login host with no queue, unless the system does not allow one (`canExec`
false); then it runs as a `BATCH` job on a Slurm system, and waits in the
queue. `describe` says which in `info.facts.exec_job_type`. The command runs in
relay's work directory; the site keeps only as much of its output as relay
reads, and relay removes the job's directory once it has read it.

## Packages

The site's JARVIS root is under relay's work directory, `<work dir>/jarvis`,
and relay sets it up, lists its packages and adds a repository through `exec`,
as on every adapter (see
[operations](../operations.md#the-sites-jarvis-root-and-its-packages)). On a
package `run` (and an install's check and plan) is the same JARVIS job as on
the ssh adapter: relay writes the pipeline and its site script into the job
directory with Tapis Files, then runs the site script's `jarvis ppl submit`
through `exec`. JARVIS submits to the system's batch scheduler, or to PSI/J's
local executor on a system with none. `status`, `cancel`, `stop` and moving
the job between queues run `jarvis execution` through `exec` too, so each one
is a short launcher job. A package service is served by the site's `ssh` or
`worker` alternative, as every `serve` is.

Tapis stops any job that runs past its time limit, and gives a job 10 minutes
when none is asked for. So relay always asks for one. A job with a duration
gets that, rounded up to whole minutes. A job with none gets its queue's own
limit, or, where the queue has no limit or there is no queue, a year.

Relay keeps no connection to Tapis between calls, so it can restart at any
time. After a restart, `find` searches Tapis for a job whose description holds
the operation id, so a retried `run` never submits a second copy.

## Files

The `files` tool works through Tapis Files and follows relay's
[files contract](../files.md). Relative paths are under the site's work
directory. Absolute paths are the host's own paths: relay maps them to Tapis
paths through the system's `rootDir`, and refuses one outside it with
`path_not_found`. A `rootDir` that is not a plain path (one Tapis works out on
the host) refuses with `invalid_config`. Tapis does
not report a home directory, so `~` is your home only on a host relay
registered, where the work directory is `~/.clio-relay/tapis`. On any other
system, `~` is the work directory.

- `list` returns every entry, however many pages Tapis takes.
- `read` with an offset asks Tapis for that byte range only.
- A large upload comes in chunks of up to 4 MiB. Tapis Files cannot append, so
  relay keeps the chunks in its own state directory and sends the whole file
  to Tapis once, with the last chunk. An upload that stopped resumes from what
  relay holds, even after relay restarts; `stat` on `<path>.relay-part` gives
  that size.
- A write never lands inside a directory standing at the path, and never
  replaces a file unless `overwrite` is set, checked again at the last chunk.
- `delete` removes a link as itself, never what it points at. It refuses the
  work directory and every directory above it, and a recursive delete is one
  Tapis call.

## Guarded sites

On `vista` and `stampede3` the site is guarded: `run` takes a reviewed
clio-jarvis package by name, version and digest, with its parameters. A
free-form executable is refused with `policy_refused` before anything reaches
Tapis. Every job on these sites asks you before it spends allocation hours.
Both are Slurm systems, where JARVIS submits the package job to Slurm.

## Refusals you may see

| Refusal | What it means |
| --- | --- |
| `needs_sign_in` | Sign in to TACC on the page it names, then try again |
| `invalid_config` | The site has no Tapis system yet, and registering it on first use failed or the site names a `system_id`. Add or switch to the site again, or run `register-host`. On a TACC host, set `system_id` to one of the `candidates` it lists |
| `policy_refused` | A free-form executable on a guarded site |
| `submit_failed` | Tapis refused the job or the credential. The message carries Tapis's own words |
| `verb_not_implemented` | A free-form `run` on a system whose scheduler is not Slurm, which relay does not run its own jobs on yet, or `serve` on a site with no `ssh` or `worker` alternative |

## Testing without TACC

`tests/fixtures/tapis_stub` is a Tapis stub that answers the calls this
adapter makes, with Tapis's envelope. It runs jobs on its own machine and
really logs in over SSH to check a credential. Start it on its own with
`python -m tests.fixtures.tapis_stub --port 8765` and point a site's
`base_url` at it. `POST /_stub/device/approve` with
`{"user_code": "...", "username": "..."}` stands in for you on TACC's page.
