# clio-relay

Private relay for running Ares work from CLIO without putting application state in the network relay.

`frp` is used only as a byte transport. `clio-core` queue records are the durable state source. JARVIS-CD owns deployment, scheduler submission, provenance, and output collection.

## Roles

- `desktop`: submits work, exposes HTTP/MCP-facing tools, and drains cursors.
- `ares`: leases queued work, materializes JARVIS-CD runs, and streams events/artifacts back.
- `relay-host`: renders `frps` configuration only. It stores no job state.

## Quickstart

```powershell
uv sync
uv run clio-relay init
uv run clio-relay install-frp
uv run clio-relay ares bootstrap --ssh-host ares
uv run clio-relay relay-host render-frps-config --token $env:CLIO_RELAY_FRP_TOKEN
uv run clio-relay endpoint status
```

Submit a JARVIS pipeline intent:

```powershell
uv run clio-relay job submit --cluster ares --jarvis-yaml .\pipeline.yaml
uv run clio-relay job watch <job-id>
```

Live acceptance requires `CLIO_RELAY_CORE_DIR`, `CLIO_RELAY_FRPS_ADDR`, `CLIO_RELAY_FRP_TOKEN`, `jarvis`, `frpc`, and the Ares shell environment used interactively.
