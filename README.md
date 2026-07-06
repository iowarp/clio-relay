# clio-relay

Private relay for running configured cluster work from CLIO without putting application state in the network relay.

`frp` is used only as a byte transport. The frpc-to-frps protocol is configurable: use `wss` for Cloudflare-backed homelab routing now, and switch to `tcp` later when a cloud or institutional relay host provides raw TCP. `clio-core` queue records are the durable state source. JARVIS-CD owns deployment, scheduler submission, provenance, and output collection.

## Roles

- `desktop`: submits work, exposes HTTP/MCP-facing tools, and drains cursors.
- `worker`: leases queued work for a configured cluster, materializes JARVIS-CD runs, and streams events/artifacts back.
- `relay-host`: renders `frps` configuration only. It stores no job state.

## Quickstart

```powershell
uv sync
uv run clio-relay init
uv run clio-relay install-frp
uv run clio-relay cluster bootstrap --cluster ares
uv run clio-relay relay-host render-frps-config --token $env:CLIO_RELAY_FRP_TOKEN
uv run clio-relay relay-host render-frpc-config --cluster ares --token $env:CLIO_RELAY_FRP_TOKEN --local-port 8848 --secret-key $env:CLIO_RELAY_STCP_SECRET
uv run clio-relay endpoint status
```

Submit a JARVIS pipeline intent:

```powershell
uv run clio-relay job submit --cluster ares --jarvis-yaml .\pipeline.yaml
uv run clio-relay job watch <job-id>
```

Live acceptance requires `CLIO_RELAY_CORE_DIR`, `CLIO_RELAY_FRPS_ADDR`, `CLIO_RELAY_FRP_TOKEN`, `jarvis`, `frpc`, and the target cluster shell environment used interactively.
