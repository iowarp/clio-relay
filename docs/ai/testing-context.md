# Testing Context

Local tests are necessary but not sufficient for transport or cluster behavior.

## Local Gates

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

## Live Acceptance

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

## Recent Live Evidence

The current release-candidate evidence is `release-validation-0.9.17.md`.
Use that file as the authoritative live evidence record for the 0.9.17
candidate.

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

## Machine-readable evidence

`clio-relay live-test` writes a versioned JSON report by default under
`.clio-relay/validation-reports`. The report is canonical; an optional Markdown
file is only a view. Reports contain package/build/install identity, checks,
relay and scheduler jobs, sessions/connectors, artifact and log references,
cleanup policy, residual resources, and partial failure evidence.

Use `clio-relay release validate-local` for the local quality report. Candidate
wheel reports are sealed before PyPI publication and remain valuable regression
evidence, but the final
`clio-relay release gate --policy docs/release-gate-1.0.yaml` matrix accepts
only reports produced through the published `uvx --from clio-relay==<version>`
path. A checkout or local wheel report is diagnostic evidence, not proof of a
released `uvx` path.

The pending lifecycle and scheduler-provider procedure is
`lifecycle-scheduler-live-validation.md`. Candidate-wheel evidence can gate
artifact publication, but only a repeated released-`uvx` run satisfies the
final release gate. A checkout run satisfies neither gate.
