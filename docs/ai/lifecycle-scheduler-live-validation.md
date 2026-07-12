# Lifecycle and scheduler live validation

Status: pending for the next release candidate. Local tests are not live evidence.

Run every command through the exact wheel or released `uvx` artifact being evaluated.
Set `$relay` to that artifact, for example:

```powershell
$relay = (Resolve-Path .\dist\clio_relay-1.0.0rc1-py3-none-any.whl).Path
$runId = [guid]::NewGuid().ToString("N")
$reportRoot = Join-Path ".clio-relay\validation-reports" $runId
$installSource = "wheel:$([Uri]::new($relay).AbsoluteUri)"
New-Item -ItemType Directory -Force $reportRoot | Out-Null
uvx --python 3.12 --from $relay clio-relay --help
```

The run directory and each command's canonical report id prevent concurrent or
repeated acceptance runs from overwriting evidence. Use the same run id only for
commands that belong to this acceptance attempt.

## Provider validation on Ares

The local Ares cluster definition must explicitly contain
`"scheduler_provider": "slurm"`. Run the deterministic provider acceptance. It
submits one uniquely named bounded job in a real held state, observes pending,
releases only that id, requires a node-backed allocation and a fresh running
observation, then waits for completion:

```powershell
uvx --python 3.12 --from $relay clio-relay scheduler validate-lifecycle `
  --cluster ares `
  --run-seconds 30 `
  --report "$reportRoot\ares-scheduler-lifecycle.json" `
  --validation-launcher uvx `
  --validation-install-source $installSource `
  --validation-artifact $relay
```

The JSON must name `scheduler=slurm`, preserve the exact job id, and contain passing
`scheduler.pending`, `scheduler.allocation-proven`, `scheduler.running`, and
`scheduler.completed` checks in timestamp order. If any phase fails, the orchestrator
cancels only its recorded job id and records cancellation or a residual resource in
the same report.

Also run a real JARVIS scheduled pipeline. Its durable job metadata and
`runtime-metadata.json` artifact must report the provider, scheduler job id, live
phase, scheduler script, allocation, and terminal state from authenticated runtime
metadata. `legacy_stdout` is a failed acceptance for a path that supports the
structured contract. Run it through `live-test` so success and partial failure are
both recorded canonically:

```powershell
uvx --python 3.12 --from $relay clio-relay live-test `
  --cluster ares `
  --jarvis-yaml .\path\to\site-scheduler-acceptance.yaml `
  --report "$reportRoot\ares-jarvis-scheduler.json" `
  --validation-launcher uvx `
  --validation-install-source $installSource `
  --validation-artifact $relay `
  --require-structured-runtime-metadata
```

## Owned lifecycle validation on Ares and homelab

Repeat this section for every release-policy target that requires lifecycle proof.
Use unique session ids and ports. Start one relay job that remains active during
detach and one owned gateway runtime created with `--owner-session-id` and the exact
generation returned by `session status`.

```powershell
$cluster = "ares"
$sessionId = "lifecycle-$cluster-$runId"
uvx --python 3.12 --from $relay clio-relay session start `
  --cluster $cluster --session-id $sessionId --remote-api-port 18921
$sessionStatus = uvx --python 3.12 --from $relay clio-relay session status `
  --cluster $cluster --session-id $sessionId | ConvertFrom-Json
$generationId = $sessionStatus.session_generation_id

# Use a site-owned generic ServiceRuntimeSpec; application behavior is not relay core.
$runtimeSpec = ".\path\to\site-gateway-runtime.json"
$gateway = uvx --python 3.12 --from $relay clio-relay gateway start-runtime `
  --cluster $cluster --name "lifecycle-$runId" --runtime-json-file $runtimeSpec `
  --owner-session-id $sessionId --owner-session-generation-id $generationId `
  --validation-report "$reportRoot\$cluster-gateway-start.json" `
  --validation-launcher uvx --validation-install-source $installSource `
  --validation-artifact $relay | ConvertFrom-Json

uvx --python 3.12 --from $relay clio-relay session detach `
  --cluster $cluster --session-id $sessionId `
  --validation-report "$reportRoot\$cluster-detach.json" `
  --validation-launcher uvx --validation-install-source $installSource `
  --validation-artifact $relay
uvx --python 3.12 --from $relay clio-relay session status `
  --cluster $cluster --session-id $sessionId
uvx --python 3.12 --from $relay clio-relay session teardown `
  --cluster $cluster --session-id $sessionId --keep-jobs --keep-scheduler-jobs `
  --validation-report "$reportRoot\$cluster-teardown.json" `
  --validation-launcher uvx --validation-install-source $installSource `
  --validation-artifact $relay
```

Acceptance requires:

- detach stops only desktop-owned connectors and retains the remote API, remote
  connector, gateway record, active relay job, scheduler job, and worker;
- teardown stops verified API and connector process groups and closes owned gateway
  records;
- omitted cancellation flags leave both relay and scheduler jobs active;
- `residual_resources` and `errors` are empty after requested teardown;
- PID files and metadata cannot authorize a process after PID reuse, token mismatch,
  command mismatch, or process-start mismatch;
- `session status` reports the API stopped after teardown;
- direct remote process inspection finds no verified owned API/frpc process left for
  the session id.

Run a second dedicated session with `--cancel-jobs --cancel-scheduler-jobs`, and
submit its bounded validation job through that owned session API so the server
stamps the durable job ownership. Verify that an unrelated same-cluster sentinel
job remains untouched and that only the owned scheduler id reaches the provider.
The canonical report must contain the exact owned relay and scheduler ids and a
confirmed canceled scheduler phase. Run a third teardown with `--stop-worker`, then
restart the worker before subsequent tests. Give every teardown its own path under
`$reportRoot` and pass the same four provenance options shown above; do not capture
the operational JSON with `Tee-Object` as a substitute for a canonical report.

## Bootstrap and dedicated gateway reports

Bootstrap is acceptance-capable and writes a collision-resistant canonical report
by default. For a release run, supply a named path and artifact provenance so the
report can be archived with the rest of the run:

```powershell
uvx --python 3.12 --from $relay clio-relay cluster bootstrap `
  --cluster $cluster --relay-wheel $relay `
  --report "$reportRoot\$cluster-bootstrap.json" `
  --validation-launcher uvx --validation-install-source $installSource
```

Run a separate bounded gateway lifecycle when the `gateway-runtime` release-policy
scenario is required. `start-runtime` and `stop-runtime` each write a canonical
report by default; explicit paths keep the paired evidence easy to review:

```powershell
$dedicatedGateway = uvx --python 3.12 --from $relay clio-relay gateway start-runtime `
  --cluster $cluster --name "gateway-$runId" --runtime-json-file $runtimeSpec `
  --validation-report "$reportRoot\$cluster-gateway-dedicated-start.json" `
  --validation-launcher uvx --validation-install-source $installSource `
  --validation-artifact $relay | ConvertFrom-Json
uvx --python 3.12 --from $relay clio-relay gateway stop-runtime `
  $dedicatedGateway.session_id --cluster $cluster --keep-scheduler-job `
  --validation-report "$reportRoot\$cluster-gateway-dedicated-stop.json" `
  --validation-launcher uvx --validation-install-source $installSource `
  --validation-artifact $relay
```

Archive every JSON file under `$reportRoot`, the exact artifact hash/version, cluster
registry redacted of secrets, remote package version, scheduler job ids, process
scans, and timestamps. Do not claim a policy target live-tested until all evidence
required for that target was produced from the release artifact.

## Canonical release check ids

Lifecycle and scheduler reports use these stable check ids so the release policy can
require evidence without parsing prose:

- `cleanup.detach`
- `cleanup.jobs-preserved-default`
- `cleanup.relay-session`
- `cleanup.connectors`
- `cleanup.gateway-record`
- `cleanup.no-owned-resources`
- `cleanup.explicit-job-cancel`
- `gateway.submit`
- `gateway.allocated`
- `gateway.ready`
- `gateway.connect`
- `gateway.stop-connectors`
- `gateway.jobs-preserved-default`
- `gateway.closed-record`
- `scheduler.pending`
- `scheduler.allocation-proven`
- `scheduler.running`
- `scheduler.completed`
- `scheduler.structured-metadata`

`SessionLifecycleReport.to_cleanup_evidence()` and
`ServiceRuntimeStopResult.to_cleanup_evidence()` populate the shared
`CleanupEvidence` shape. Their `validation_resources()` methods populate the shared
resource list, including ownership, outcome, location, and residual state.
