# AI Context

These files are for coding agents and review agents. Human-facing docs should stay short and readable.

Read in this order:

1. `../connection-model.md`
2. `system-context.md`
3. `testing-context.md`
4. `interface-context.md`
5. `release-validation-0.9.17.md`
6. `../../README.md`
7. `../architecture.md`
8. `../operations.md`

`../connection-model.md` is normative and comes first: one link per connection,
the ssh budget, the 2FA operating assumption, reconnect, one local relay for
many remotes, and relay-owned input staging. Where another page or the shipped
code disagrees with it, it is the other page or the code that is wrong.

Do not treat examples as hardcoded product semantics. Examples show one configured target or workload.

Live evidence files under this directory record what one run of one release
actually did. Their commands are evidence, not interface guidance; an `ssh` or
`scp` invocation in a validation transcript is harness scaffolding and must
never be copied into product code, agent instructions, or a benchmark harness.
