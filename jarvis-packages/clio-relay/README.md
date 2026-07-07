# clio-relay JARVIS packages

These scripts are legacy standalone shims. The JARVIS-CD package repository registered by
`clio-relay cluster bootstrap --cluster <name>` is `jarvis-packages/clio_relay`.

The registered packages are the relay-owned execution shims referenced by generated JARVIS-CD pipelines.

They are intentionally narrow:

- `bounded-command`: execute a bounded command with optional environment, workdir, and timeout.
- `remote-agent`: run the configured agent binary with a prompt file and MCP config.
- `mcp-call`: call a remote MCP tool through a JSON-RPC stdio-compatible server command.

JARVIS-CD remains responsible for scheduler submission, environment capture, output collection, and provenance.
