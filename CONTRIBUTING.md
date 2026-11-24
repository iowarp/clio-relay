# Contributing to clio-relay

Thank you for helping. Most contributions are one of three things, and each
has a short path:

| You want to | Read |
| --- | --- |
| Add a machine relay can already reach (ssh, Tapis, Globus, IRI) | [Adding a machine or an adapter](docs/adding-an-adapter.md#add-a-machine-on-an-existing-protocol) |
| Add a new way into a facility | [Adding a machine or an adapter](docs/adding-an-adapter.md#write-an-adapter) and [the template](examples/adapter-template) |
| Fix or improve relay itself | The layout below, then [architecture](docs/architecture.md) |

## Set up

Relay uses [uv](https://docs.astral.sh/uv/) and Python 3.12.

```bash
git clone https://github.com/iowarp/clio-relay
cd clio-relay
uv sync --all-extras
uv run pytest tests/relay_next
```

## Checks

A change is ready when all of these pass, with no test skipped:

```bash
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
```

Some tests need a real site: a Slurm cluster, a Tapis tenant, an ssh host.
They take its address from the environment and fail without it; run them
where you have that site, or leave them to the maintainers.

The repository also keeps two ratchets: `scripts/check_file_size.py` (a file
stays under 800 lines) and `scripts/check_pyright_ratchet.py` (the count of
pyright errors only goes down).

CI also runs the conformance kit against the adapter template, the way its
README tells a newcomer to, so a change to the adapter contract that breaks an
outside adapter fails here first:

```bash
cd examples/adapter-template
uv run pytest
```

## Layout

Read it top-down: an MCP call enters at `surface`, the guard checks it, and an
adapter carries it to the site.

| Path | What it holds |
| --- | --- |
| `src/clio_relay/surface/` | The MCP server and its thirteen tools (the five verbs, `install`, the read tools `exec_read`, `files_read` and `packages_read`, `packages`, `cancel`, `site_clean`, `shutdown`), the service behind them, and the Compute tab |
| `src/clio_relay/guard/` | Open and guarded sites: package review, read-only exec, spend questions |
| `src/clio_relay/adapters/base.py` | The adapter contract |
| `src/clio_relay/adapters/<name>/` | One adapter each: `ssh`, `tapis`, `globus`, `iri`, `sky`, `worker` |
| `src/clio_relay/adapters/registry.py` | Finds adapters through the `clio_relay.adapters` entry points |
| `src/clio_relay/jobs/` | Job specs, handles, operation records, the content store and the files rules |
| `src/clio_relay/sites/` | `sites.yaml` and which adapter serves each verb |
| `src/clio_relay/catalog/` | The sites and skills relay ships |
| `src/clio_relay/asks/`, `identity/` | Questions for the person, and sign-ins |
| `src/clio_relay/serve/` | The `serve` verb: services and port forwarding |
| `src/clio_relay/frp_visitor/` | Relay's own desktop frp visitor (stcp over tcp, websocket and wss), run as its own process by `serve` |
| `src/clio_relay/testing/conformance/` | The conformance kit every adapter passes |
| `src/clio_relay/testing/fixture_adapter.py` | A simulated site for relay's own tests; `tests/conftest.py` registers it, since it is not a shipped entry point |
| `tests/` | Tests, one directory per area |
| `docs/` | The documentation people read; `docs/design/` holds the design record |

## Writing

- Docs are for people. Say what something does and how to use it; leave the
  history to `docs/design/relay-rework-ledger.md`.
- Headings are in sentence case.
- Nothing in core code names a particular cluster, agent or server. Sites,
  agents and transports are settings.

## Pull requests

Keep one change per pull request, say what it does and how you checked it,
and include the test that shows it works.
