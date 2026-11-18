# The tapis adapter

The `tapis` adapter runs jobs through [Tapis](https://tapis.readthedocs.io/),
TACC's API for systems, apps, jobs and files. Relay talks to the Tapis
tenant over HTTPS, and Tapis logs in to the host and runs the job. Your
machine needs no SSH access to the host while a job runs.

It serves `describe`, `run` (with `status`, `cancel` and `find` on the jobs it
starts) and `files`. `exec` and `serve` are not Tapis operations. A site that
needs them names an `ssh` or `worker` alternative, and `describe` shows which
adapter serves each verb.

The catalog has four Tapis sites:

| Site | Host | How Tapis logs in | Jobs |
| --- | --- | --- | --- |
| `chameleon-tapis` | A Chameleon node with a floating IP | A key pair relay makes | `FORK`, open |
| `ares-tapis` | IIT's Ares | A key pair relay makes | `BATCH` on Slurm, open |
| `vista` | TACC Vista | TACC's trust manager | `BATCH`, guarded |
| `stampede3` | TACC Stampede3 | TACC's trust manager | `BATCH`, guarded |

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
name `clio-relay`. They are never shown to the model, logged or written to
relay's files. The short code is meant for you to read and is not a secret.

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

The device code sign-in needs an OAuth client in the `tacc` tenant. Until relay
ships one, set it in the environment relay runs in:

| Variable | What it is |
| --- | --- |
| `CLIO_RELAY_TACC_CLIENT_ID` | The client's id |
| `CLIO_RELAY_TACC_CLIENT_KEY` | The client's key. Needed only for refresh |
| `CLIO_RELAY_TACC_BASE_URL`, `CLIO_RELAY_TACC_TENANT_ID` | Another Tapis tenant |

A site's `client_id` setting takes precedence over the environment.

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
  spend: {ask: never}
```

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `base_url` | `https://tacc.tapis.io` | The Tapis tenant's address |
| `tenant_id` | `tacc` | The tenant |
| `authn` | `pki` | `pki`: relay makes a key pair for a host you reach over SSH. `tms`: TACC's trust manager makes and holds the key |
| `system_id` | `clio-relay.<username>.<site id>` | The Tapis system. Set it to use a system someone else registered |
| `work_dir` | the system's job working directory | Relay's directory on the system, as a Tapis path |
| `launcher` | `open` | `open` runs the job's executable. `guarded` runs reviewed packages only |
| `client_id` | the environment | Relay's OAuth client, when not the shipped one |

## Registering a host

Tapis runs jobs on a *system*: a host it can log in to. Relay registers one
system for you per site, once.

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
   Tapis logical queue, and jobs run as `BATCH` jobs. A host with no scheduler
   runs `FORK` jobs.
3. It makes a new RSA key pair, adds the public key to your
   `~/.ssh/authorized_keys` with the comment `clio-relay-tapis:<system id>`,
   and gives the pair to Tapis as your credential for the system.
4. It deletes the private key from your machine. Only Tapis keeps it.
5. Tapis checks that it can log in. If it cannot, relay takes the key line
   back out and reports what Tapis said.

The host must accept SSH logins from TACC with a key. A host that asks for a
second factor on every login cannot be registered this way.

`unregister-host` removes the credential, the system relay made, and the key
line with relay's comment. It never removes a system relay did not make.

### TACC hosts (`authn: tms`)

TACC's own hosts ask for a second factor, so relay makes no key. It asks Tapis
to create the credential with TACC's trust manager (`createTmsKeys`), which
holds the key on TACC's side. The systems are TACC's own; relay registers only
your credential and never creates or deletes the system. You need an
allocation on the host.

## How a job runs

1. Relay registers its launcher once per system as a Tapis app named after the
   system. The app is a ZIP-runtime app whose archive is a small gzip tar
   holding one shell script. Relay uploads the archive to `<work dir>/launcher/`
   through Tapis Files. The app version holds the script's digest, so a new
   launcher is a new app version.
2. Inputs are staged by content into `<work dir>/content/` through Tapis Files.
   An input already there is not sent again. The job lists each input as a
   `tapislocal` file input, so Tapis does not copy it.
3. Relay submits the job with `submitJob`. The job's description carries the
   relay operation id, and the app arguments carry the job spec as one base64
   argument, so nothing is quoted through a shell.
4. The launcher links the inputs into the job's directory, runs the
   executable (or, on a guarded site, the reviewed package) and writes the
   exit code to `relay.exitcode`.
5. `status` reads the Tapis job status, and the exit code once the job ends.
   `cancel` calls `cancelJob`.

Relay keeps no connection to Tapis between calls, so it can restart at any
time. After a restart, `find` searches Tapis for a job whose description holds
the operation id, so a retried `run` never submits a second copy.

## Guarded sites

On `vista` and `stampede3` the site is guarded: `run` takes a reviewed
clio-jarvis package by name, version and SHA-256, with its parameters. A
free-form executable is refused with `policy_refused` before anything reaches
Tapis. Every job on these sites asks you before it spends allocation hours.

## Refusals you may see

| Refusal | What it means |
| --- | --- |
| `needs_sign_in` | Sign in to TACC on the page it names, then try again |
| `invalid_config` | The site has no Tapis system yet. Run `register-host` |
| `policy_refused` | A free-form executable on a guarded site |
| `submit_failed` | Tapis refused the job or the credential. The message carries Tapis's own words |
| `verb_not_implemented` | The system uses a scheduler other than Slurm, which relay does not run jobs on yet |

## Testing without TACC

`tests/fixtures/tapis_stub` is a Tapis stub that answers the calls this
adapter makes, with Tapis's envelope. It runs jobs on its own machine and
really logs in over SSH to check a credential. Start it on its own with
`python -m tests.fixtures.tapis_stub --port 8765` and point a site's
`base_url` at it. `POST /_stub/device/approve` with
`{"user_code": "...", "username": "..."}` stands in for you on TACC's page.
