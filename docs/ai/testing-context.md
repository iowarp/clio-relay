# testing context

Local tests are necessary but not sufficient for transport or cluster behavior.

A harness may place fixtures on an evidence cluster at bring-up, but it must
never move a run input out of band. Copying scientific inputs to the cluster
before a run masks the staging seam and makes the results describe the harness
rather than relay; that bypass has already invalidated a benchmark campaign and
is tracked as [#177](https://github.com/iowarp/clio-relay/issues/177). Every
harness that submits work asserts the invariants in `docs/connection-model.md`,
not the ones its own scaffolding happens to satisfy.

## local gates

Run:

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
uv build
```

For CI parity, also verify the built artifacts:

```powershell
uvx twine check dist/*
```

The rework's relay tests are split by area. `tests/relay_next` covers the core
service, server, routing and the conformance kit. `tests/asks` covers asks and
identities, `tests/relay_ssh` the ssh adapter and `tests/relay_tapis` the Tapis
adapter. The ssh and asks tests reach the W2 fixture sites named in the file
that `CLIO_RELAY_FIXTURES_ENV` points to.

## ci

`.github/workflows/ci.yml` runs `uv run ruff check`, `uv run ruff format --check`,
`uv run pyright` and `uv run pytest` on the tree. `.github/workflows/release.yml`
runs on a `v*` tag, builds the wheel and source distribution, and publishes them.
Neither is a release gate: release qualification is the live LQ rows.

## live acceptance

A feature that depends on a cluster, transport, scheduler, agent process, or MCP call is not done until that path has been live-tested.

The expected Ares acceptance includes:

- bootstrap or update the cluster deployment.
- start or restart the worker service.
- submit a real JARVIS pipeline.
- verify terminal state, events, task records, stdout, stderr, artifacts, and provenance.
- verify package-aware progress when the pipeline uses a supported package.
- verify agent MCP submission when agent behavior is in scope.
- verify transport through the configured path when transport behavior is in scope.
- verify detach and teardown behavior when lifecycle behavior is in scope.

For endpoint/session, remote-MCP, staging, or provenance changes, current live
evidence must also prove the relevant boundary before it is claimed:

- a persistent endpoint recovers from a process exit across desktop disconnect,
  while an intentional service stop remains stopped and is reported distinctly;
- a nonterminal session start remains unusable and resumable through its exact
  status selector, and a pre-metadata failure converges to verified
  `failed_cleaned` cleanup without default job cancellation;
- a generic remote MCP call runs in endpoint containment with
  `outer_jarvis_pipeline=false` and creates no scheduler allocation;
- a package-declared v3.6 local file is hash-pinned, ingested, rewritten, and
  inherited by the exact later JARVIS run after an MCP reconnect, with a
  non-empty `used_artifact_refs` whose digest matches the client's bytes and no
  out-of-band copy anywhere in the proven path;
- the connection opens no new ssh connections after bring-up: zero in the frp
  modes and one held forward in the ssh fallback, counted two-sided across an
  arbitrary number of operations, watches, and recoveries; and
- a nondefault input policy survives session start, status/retry reconstruction,
  API launch, and reconnect, while a changed policy is rejected without explicit
  replacement;
- default teardown preserves and reports many exact scheduler identities through
  bounded provider batches rather than one remote process per job;
- edge provenance and any transform record round-trip through the authenticated
  released API without changing legacy dependency identities.

These are acceptance requirements, not evidence that the current candidate has
already passed them. Record the actual released-artifact run before making a
live-validation claim.

## recent live evidence

Historical release-candidate evidence is retained in
`release-validation-0.9.17.md`. Use that file only as the authoritative record
for the 0.9.17 candidate; it is not evidence for the current 1.0 gate.

The 0.9.17 validation includes:

- wheel-backed packaged bootstrap from a temporary directory with no `.git`.
- Ares worker restart with `clio_relay.__version__ == 0.9.17`.
- Homelab WSS/STCP, XTCP, and SSH-forward transport checks, followed by clean
  probe-process and metadata cleanup scans.
- Ares application-package acceptance with WSS relay, XTCP direct transport,
  stdout, stderr, artifacts, provenance, and package-owned progress.
- Ares remote-agent MCP child application submission.
- forced SLURM pending state and relay cancel propagation.
- ParaView-style gateway create, update, get, and close record lifecycle.
- task timeline CLI record/read and HTTP, SSE, and WebSocket replay over an
  owned SSH-forwarded Ares API session.

Historical validation files remain useful for debugging regressions, but they
are not current proof for a release candidate.

Refresh this evidence when changing the relevant code. Do not present old live evidence as current proof after transport, lifecycle, worker, or acceptance code changes.

## machine-readable evidence

`clio-relay live-test` writes a versioned JSON report by default under
`.clio-relay/validation-reports`. The report is canonical; an optional Markdown
file is only a view. Reports contain package/build/install identity, checks,
relay and scheduler jobs, sessions/connectors, artifact and log references,
cleanup policy, residual resources, and partial failure evidence.

The report-producing acceptance commands sit under `clio-relay worker`:
`relay-host test-frpc-connection`, `test-http-transport`, `test-direct-transport`, and
`test-ssh-transport`; `cluster bootstrap`; and `session detach` and
`session teardown`. Every invocation writes a collision-resistant default JSON report on
success and after preflight or runtime failure unless the operator supplies an
explicit path.

The 1.0 release gate is gone. Release qualification is the LQ and RL rows in
`docs/design/relay-rework-plan.md`, proven by live runs.
