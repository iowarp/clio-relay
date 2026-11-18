# AI Context

These files are for coding agents and review agents. Human-facing docs should stay short and readable.

Read in this order:

1. `../design/relay-rework-plan.md`: the contract (RL, HS and LQ rows).
2. `interface-context.md`: the command line, the MCP surface and the extension points.
3. `../design/relay-rework-ledger.md`: the published interfaces and the decisions behind them.
4. `system-context.md`
5. `testing-context.md`
6. `../../README.md`
7. `../architecture.md`
8. `../operations.md`: the `worker` adapter's operator commands.

Where another page or the shipped code disagrees with the contract, the other page or the code is wrong.

Do not treat examples as hardcoded product semantics. Examples show one configured target or workload.

Live evidence files under this directory record what one run of one release
actually did. Their commands are evidence, not interface guidance; an `ssh` or
`scp` invocation in a validation transcript is harness scaffolding and must
never be copied into product code, agent instructions, or a benchmark harness.
