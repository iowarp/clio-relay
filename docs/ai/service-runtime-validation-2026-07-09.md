# service runtime validation, 2026-07-09

This note records the first live validation of the generic `ServiceRuntimeSupervisor`.
It proves scheduler allocation, relay connector ownership, desktop-local reachability,
and cleanup. It does not prove the final push-streaming contract; that contract
supersedes this pull-render validation.
It also used a direct scheduler wrapper rather than a JARVIS package-owned runtime
driver, so it is not acceptance evidence for JARVIS-owned deployment semantics.

## implementation under test

- `src/clio_relay/service_runtime.py`
- `src/clio_relay/models.py` `ServiceRuntimeSpec`
- CLI:
  - `clio-relay gateway start-runtime`
  - `clio-relay gateway stop-runtime`

The runtime supervisor is generic. ParaView was used only as the live application.

## live application

- Cluster: `ares`
- Dataset: SciVis 2018 Deep Water Impact, yB31
- Descriptor dataset id: `scivis2018-deep-water-impact-yb31`
- Pipeline: `impact_isosurfaces`
- Remote descriptor:
  `/home/jcernudagarcia/.local/share/clio-relay/service-runtime/svc-pv-20260708194754/descriptor.json`
- Remote service script:
  `/home/jcernudagarcia/slurm/relay-live/paraview_live_service.py`
- Remote Slurm wrapper:
  `/home/jcernudagarcia/slurm/relay-live/slurm_paraview_live_service.sbatch`

## managed runtime session

- Session: `gateway_9634f89df7e54d98948dcec786514281`
- Slurm job: `21721`
- Allocated node: `ares-comp-29`
- Remote service: `ares-comp-29:18831`
- Desktop bind: `127.0.0.1:28831`
- Transport: `frp-stcp-wss`
- Remote connector PID: `3629614`
- Desktop connector PID: `55716`

## compatibility endpoint evidence

Requests were made to the desktop-local relay endpoint, not to the cluster node directly.
These were compatibility pull requests against the application-specific `/render` endpoint:

- `GET http://127.0.0.1:28831/healthz`
  - `status=ready`
  - `dataset_id=scivis2018-deep-water-impact-yb31`
- `GET http://127.0.0.1:28831/state`
  - `pipeline=impact_isosurfaces`
  - `timesteps=269`
- `GET http://127.0.0.1:28831/render?timestep=5&field=pressure&yaw=12&zoom=1.0`
  - wrote `.clio-relay/live/service-runtime-paraview/impact-yb31-frame-0005.png`
  - size `29084` bytes
- `GET http://127.0.0.1:28831/render?timestep=30&field=pressure&yaw=12&zoom=1.0`
  - wrote `.clio-relay/live/service-runtime-paraview/impact-yb31-frame-0030.png`
  - size `16456` bytes
- `GET http://127.0.0.1:28831/render?timestep=120&field=pressure&yaw=12&zoom=1.0`
  - wrote `.clio-relay/live/service-runtime-paraview/impact-yb31-frame-0120.png`
  - size `16456` bytes
- `GET http://127.0.0.1:28831/events`
  - reported `service_ready`
  - reported three `frame_rendered` events for timesteps `5`, `30`, and `120`

The timestep 5 frame was visually inspected and was nonblank.

## remaining push-stream validation

The corrected runtime contract is push-first:

- `stream_mode=push`
- caller-defined `stream_path`
- optional `event_stream_path`
- optional named `compatibility_paths`
- JARVIS/package-owned submit/status/cancel commands that emit structured JSON

A follow-up live validation must subscribe to `stream_url` once and prove that the remote
application pushes payloads through frp without per-frame desktop pull requests. The
application-specific `/render` endpoint may remain as a compatibility path, but it is not
the default runtime semantics.

The follow-up validation must also use a JARVIS package or pipeline as the deployment
driver. The relay core should consume package-emitted JSON records such as
`{"scheduler_job_id":"...","service_host":"..."}` and must not parse Slurm or
application stdout directly.

## cleanup

`clio-relay gateway stop-runtime gateway_9634f89df7e54d98948dcec786514281 --cluster ares --cancel-scheduler-job` was run.

Post-cleanup checks:

- Local TCP probe to `127.0.0.1:28831` failed as expected.
- Local desktop connector PID `55716` was no longer present.
- Remote frpc PID `3629614` was no longer present.
- Slurm job `21721` was canceled.

The initial failed run before the Windows CRLF control-channel fix left `gateway_ca7a3ede7c4345b690557ac62404ea8d`; it had no scheduler job id and was closed.
