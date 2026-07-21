# testing context

Local tests are necessary but not sufficient for transport or cluster behavior.

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

## build-once release CI

The trusted release candidate is produced only by the primary Ubuntu/Python
3.12 leg of a `merge_group` run. The other five matrix legs pass the downloaded
wheel, source distribution, and canonical checksum file through
`release validate-local --prebuilt-artifact-dir`; that mode rejects extra,
missing, or changed files and cannot execute either `uv build` path. The final
CI seal binds all six machine-readable reports to one distribution digest map,
tested merge-group commit, Git tree, queued PR, run, and attempt.

A protected-main tag must identify the merged PR commit and the same tested Git
tree. The tag workflow is a lightweight binding workflow: it performs no sync,
test, component fetch, artifact build, or candidate execution. Repository rules
and merge-queue behavior require a live disposable canary before an owner
enables them; checkout tests alone cannot prove GitHub's eventual merge identity.

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

The complete report-producing acceptance CLI inventory is `release validate-local`;
`relay-host test-frpc-connection`, `test-http-transport`, `test-direct-transport`, and
`test-ssh-transport`; `remote-mcp validate`; `cluster bootstrap`; `session detach`
and `session teardown`; `queue validate`; `scheduler validate-lifecycle`; `gateway
start-runtime`, `detach-runtime`, and `stop-runtime`; `jarvis-mcp-validate`; and
`live-test`. Every invocation writes a collision-resistant default JSON report on
success and after preflight or runtime failure unless the operator supplies an
explicit path. `release gate` consumes those reports; it is not an acceptance-report
producer.

Long scheduler residence is represented as `pending`, never as workload failure.
Resume `live-test` with `--resume-report`; the new sibling report observes the
same run, job, idempotency key, and phase-specific JARVIS identities without a
TTL, resubmission, or scheduler cancellation. Pending reports are diagnostic and
are rejected by the release gate until the exact run produces a passed report.

Use `clio-relay release validate-local` for the local quality report. Candidate
wheel reports are sealed before PyPI publication and remain valuable regression
evidence, but the final
`clio-relay release gate --policy docs/release-gate-1.0.yaml` matrix accepts
only reports produced through a persistent, exact
`uv tool install --python 3.12 --no-config clio-relay==<version>` installation.
A checkout, local wheel, or temporary `uvx` report is diagnostic evidence, not
proof of the released persistent-tool path.

The pending lifecycle and scheduler-provider procedure is
`lifecycle-scheduler-live-validation.md`. Candidate-wheel evidence can gate
artifact publication, but only a repeated released persistent-tool run
satisfies the final release gate. A checkout run satisfies neither gate.
