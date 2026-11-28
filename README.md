<p align="center">
  <img src="docs/assets/clio-relay-banner.png" alt="clio-relay banner">
</p>

<h1 align="center">clio-relay</h1>

`clio-relay` runs work on the computers you use: your own machine, a cluster
you reach over ssh, a Tapis tenant, a Globus Compute endpoint, an IRI facility
or a cloud through SkyPilot. It is a standard MCP server, so any agent that
speaks MCP can use it: Claude Code, Copilot CLI, Codex, CLIO, or a plain `mcp`
client. It is also a command line for people.

## How it works

Relay keeps a list of sites in `sites.yaml`. Each site names an adapter, which
knows how to reach it, and a policy that says what may run there. Every site
answers the same five verbs:

| Verb | What it does |
| --- | --- |
| `describe` | Lists the sites, describes one site, or observes one operation |
| `run` | Submits a job, written as a PSI/J job spec, and follows it to the end |
| `exec` | Runs one short command and returns its output |
| `files` | Lists, reads, writes, deletes and stages files, with resumable chunks for large ones |
| `serve` | Starts a long-running service and forwards its ports to you |

`run` and `serve` are MCP tasks when the client supports the tasks extension:
the client gets progress on the task, can leave and come back, and answers
questions (a one-time code, a spend approval) through the task. A client
without tasks gets an operation id instead, which `describe(job=...)` follows.
`install` installs a package's software on a site ahead of any run, with the
same questions as a run's install, and launches nothing.
On a site whose JARVIS root relay has not set up yet, it sets that up
(jarvis-cd and clio-jarvis, see [operations](docs/operations.md#the-sites-jarvis-root-and-its-packages)).
See [MCP tasks](docs/mcp-tasks.md). `files` behaves the same on every site; see
[files](docs/files.md).

A site that is `guarded` runs only reviewed packages, and asks before it spends
an allocation. See [guarded sites](docs/guarded-sites.md).

## Install

```powershell
uv tool install --python 3.12 --no-config clio-relay
```

Add the extras for the adapters you need, for example
`clio-relay[tapis,globus]`.

## Add a site

Pick one from the catalog, or describe your own:

```powershell
clio-relay site catalog
clio-relay site add-from-catalog ares
clio-relay site add my-cluster --adapter ssh --config '{"ssh_profile": "my-cluster"}' --policy open
clio-relay site list
```

`clio-relay adapters` lists the adapters installed. Each adapter page below
lists its settings. Leave out `--adapter` or `--policy` and `site add` uses the
defaults for new sites, which `clio-relay site defaults` shows and changes; the
Compute page in CLIO shares them.

## Use it from an agent

Point the agent's MCP configuration at the stdio server:

```json
{"mcpServers": {"relay": {"command": "clio-relay", "args": ["serve"]}}}
```

For Streamable HTTP, run `clio-relay serve --http`. It listens on
`127.0.0.1:8766/mcp` and requires a bearer token, which it writes to its state
directory the first time unless `CLIO_RELAY_TOKEN` is set.

## Answer what a site asks

Some sites ask for a one-time code, a sign-in or a spend approval. An agent
with tasks or elicitation shows the question to you. From a terminal:

```powershell
clio-relay asks list
clio-relay asks answer <ask-id>
clio-relay asks identities
```

## Connect to a service

`serve` returns a connect command for each running service. Run it on your
machine to forward the service's ports and hold them open:

```powershell
clio-relay connect <handle>
```

## Bring your own machine or facility

A machine you reach over ssh, or one behind Tapis, Globus Compute or IRI, is a
new site: one entry in `sites.yaml`, no code. A facility with its own API needs
an adapter: one Python class in your own package, registered by an entry
point, with relay's conformance kit to prove it works. Relay's code does not
change.

- [Adding a machine or an adapter](docs/adding-an-adapter.md): the walkthrough
- [The adapter template](examples/adapter-template): a complete adapter and a fake facility to copy
- [Contributing](CONTRIBUTING.md): setting up, the checks, and the layout

## Documentation

- [MCP tasks](docs/mcp-tasks.md): how `run` and `serve` behave as tasks, and without them
- [files](docs/files.md): paths, large files and resuming, the same on every site
- [guarded sites](docs/guarded-sites.md): what a guarded site runs, and how packages are reviewed
- [rendezvous host](docs/rendezvous-host.md): the meeting point where a service and your machine both dial out
- [adding a machine or an adapter](docs/adding-an-adapter.md)
- [architecture](docs/architecture.md)
- [operations](docs/operations.md)
- [release](docs/release.md)
- [brand prompt](docs/brand.md)
- adapters
  - [globus](docs/adapters/globus.md): jobs through Globus Compute, files through Globus Transfer
  - [iri](docs/adapters/iri.md): the IRI Facility API that NERSC, ALCF, OLCF and ESnet serve
  - [sky](docs/adapters/sky.md): SkyPilot managed jobs on clouds, Kubernetes and Slurm
  - [ssh](docs/adapters/ssh.md): any site you can reach with ssh, with or without a batch scheduler
  - [tapis](docs/adapters/tapis.md): TACC and other Tapis tenants, with no ssh from your machine
  - [worker](docs/adapters/worker.md): a small relay worker on the cluster, reached through a rendezvous server after one ssh sign-in

## Development

See [CONTRIBUTING](CONTRIBUTING.md).

```powershell
uv sync --all-extras
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```