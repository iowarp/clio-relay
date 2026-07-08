# Correction Tasks

Current status: the 0.9.7 working tree implements the local code corrections for
workload-specific bootstrap removal, scheduler provider isolation, and
package-owned LAMMPS progress parsing. Full rollout is still blocked until these
corrections pass installed-package `uvx` validation on homelab and Ares.

1. **Use the builtin JARVIS LAMMPS package as the acceptance application**
   - LAMMPS is already a builtin JARVIS-CD package under `builtin.lammps`; do not duplicate it as `clio_relay.lammps` unless upstream extension proves impossible.
   - Move LAMMPS-specific execution/progress out of `clio_relay.bounded_command`.
   - Acceptance YAML must use the builtin JARVIS `lammps` package directly, not a shell wrapper plus `progress.adapter: lammps`.
   - Configure builtin `lammps` with a meaningful input script and run it on Ares through JARVIS.
   - Capture stdout, stderr, artifacts, provenance, and progress from that real package execution.
   - `bounded_command` stays generic, with only generic regex progress as a fallback for applications without package support.

2. **Make package progress trustworthy**
   - Stop accepting arbitrary workload stdout `CLIO_PROGRESS ...` as proof of package progress.
   - Package progress must carry provenance: `source=jarvis_package`, package name, package version, run/execution id.
   - HTTP/MCP `record_progress` should be marked as external/observer progress.
   - Acceptance must reject forged stdout markers and externally recorded progress when validating package-owned progress.

3. **Harden LAMMPS parsing**
   - Require detected LAMMPS thermo headers.
   - Parse named thermo columns, especially `Step`, not "first numeric token."
   - Track active run blocks and step ranges.
   - Support `run N`, nonzero starts, repeated runs, and `reset_timestep`.
   - Emit ETA using trimmed/median step-time after warmup with confidence metadata.

4. **Fix transport probe lifecycle**
   - Remove broad `pkill`.
   - Use a unique remote API port per live test or fail clearly if occupied.
   - Clean up only processes started by the current probe.
   - Quote generated remote shell values safely.

5. **Fix timeout and cancellation semantics**
   - Kill process groups, not just direct children.
   - For SLURM-backed package execution, capture scheduler job ids.
   - On timeout/cancel, call `scancel` for scheduler jobs.

6. **Make file-backed clio-core idempotency more durable**
   - Atomic replace prevents torn JSON reads, but submission is not transactional.
   - Add idempotency reservation before job creation.
   - Add recovery for half-written submissions.
   - Add fsync if the file backend remains a supported durable backend.

7. **Add missing tests and live acceptance checks**
   - Negative test: forged `CLIO_PROGRESS` must not satisfy package-progress acceptance.
   - Test that deployed Ares package path actually loads the LAMMPS package code.
   - Parser edge-case tests for headers, repeated runs, reset timestep, malformed lines.
   - Timeout cleanup tests for process groups and SLURM cancellation.
   - Transport conflict tests proving unrelated APIs are not killed.

8. **Add Clio product semantics**
   - SSE/WebSocket monitor stream for Clio UI.
   - Agent-as-monitor trigger path: progress/event rules can submit remote-agent tasks with context.
   - NAT punching remains optional and should be designed/tested as a transport optimization with fallback to relay/STCP/queue, not mixed into the core reliability path.
