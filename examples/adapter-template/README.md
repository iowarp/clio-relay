# Adapter template

A complete clio-relay adapter for a pretend facility, Acme, that has a REST
API with an API token, a job queue, a command runner and a file system. It
passes relay's conformance kit against an in-repo fake of that API. Copy this
directory to start an adapter for your own facility.

| File | What it is |
| --- | --- |
| `src/relay_acme/adapter.py` | The adapter: its settings, sign-in, `describe`, `run`, `status`, `cancel`, `find`, `exec` and `files`. |
| `pyproject.toml` | The package, and the entry point that registers the adapter with relay. |
| `tests/fake_facility.py` | The fake Acme API the tests run against. |
| `tests/test_conformance.py` | Relay's conformance kit, pointed at the adapter and the fake. |

Run the kit from this directory:

```bash
uv run pytest
```

The walkthrough is [docs/adding-an-adapter.md](../../docs/adding-an-adapter.md).
