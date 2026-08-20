"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
from typing import cast

import clio_relay.live_acceptance_agent_prompt as live_acceptance_agent_prompt
import clio_relay.live_acceptance_browser_evidence as live_acceptance_browser_evidence
import clio_relay.live_acceptance_checkpoint as live_acceptance_checkpoint
import clio_relay.live_acceptance_handoff as live_acceptance_handoff
import clio_relay.live_acceptance_job_verification as live_acceptance_job_verification
import clio_relay.live_acceptance_models as live_acceptance_models
import clio_relay.live_acceptance_packaged_mcp as live_acceptance_packaged_mcp
import clio_relay.live_acceptance_progress as live_acceptance_progress
import clio_relay.live_acceptance_remote_io as live_acceptance_remote_io
import clio_relay.live_acceptance_secret_redaction as live_acceptance_secret_redaction
import clio_relay.live_acceptance_secure_runtime as live_acceptance_secure_runtime
import clio_relay.live_acceptance_transport as live_acceptance_transport
import clio_relay.live_acceptance_wait as live_acceptance_wait
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.live_acceptance_models import (
    CommandRunner,
    LiveAcceptanceCheckpoint,
    LiveAcceptanceOptions,
    SecureRuntimeHttpEvidence,  # noqa: F401 -- unused here; tests bare-import it from this module
    SecureRuntimeProbeConfig,  # noqa: F401 -- unused here; tests bare-import it from this module
    SecureRuntimeProtocolAdapter,  # noqa: F401 -- unused here; tests bare-import it from this module
)
from clio_relay.models import (
    JobState,
)
from clio_relay.transport_probe import (
    transport_evidence_lines_from_error,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationRecorder,
    ValidationResource,
    new_live_validation_report,
)

# Every extracted owner module below re-exports its still-needed private
# names here via qualified assignment (the cli_support._run_or_exit /
# cli.py:782 idiom): a name stays listed only if the facade body below
# calls it bare, or test_live_acceptance.py imports it directly by name --
# names whose only caller moved out with their own function (e.g. into
# live_acceptance_secure_runtime.py) are NOT re-exported here; nothing
# references them through this module anymore.

# live_acceptance_models.py
_AcceptanceObservationPending = (
    live_acceptance_models._AcceptanceObservationPending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_BrowserHttpRequestError = (
    live_acceptance_models._BrowserHttpRequestError  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_LiveAcceptancePending = (
    live_acceptance_models._LiveAcceptancePending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_LiveAcceptanceState = (
    live_acceptance_models._LiveAcceptanceState  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_ValidationLines = (
    live_acceptance_models._ValidationLines  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_acceptance_run_id = (
    live_acceptance_models._acceptance_run_id  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_configured_path = (
    live_acceptance_models._configured_path  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_remote_io.py -- _remote_env has no bare caller left in
# this file, but tests/test_remote_value_contract.py reaches it via
# vars(live_acceptance)["_remote_env"], not a `from X import Y` this
# module's own import audit would catch.
_decode_artifact_text = (
    live_acceptance_remote_io._decode_artifact_text  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_clio_json = (
    live_acceptance_remote_io._remote_clio_json  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_env = (
    live_acceptance_remote_io._remote_env  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_shell = (
    live_acceptance_remote_io._remote_shell  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_write_file = (
    live_acceptance_remote_io._remote_write_file  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_run_command = (
    live_acceptance_remote_io._run_command  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_stage_acceptance_files = (
    live_acceptance_remote_io._stage_acceptance_files  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_transport.py
_assert_direct_xtcp_acceptance = (
    live_acceptance_transport._assert_direct_xtcp_acceptance  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_http_json = (
    live_acceptance_transport._http_json  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_require_transport_secrets = (
    live_acceptance_transport._require_transport_secrets  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_cluster_deployment = (
    live_acceptance_transport._verify_cluster_deployment  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_direct_transport = (
    live_acceptance_transport._verify_direct_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_ssh_transport = (
    live_acceptance_transport._verify_ssh_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_transport = (
    live_acceptance_transport._verify_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_secret_redaction.py
_assert_secret_free_document = (
    live_acceptance_secret_redaction._assert_secret_free_document  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_secure_runtime.py owns the v3.6 secure runtime acceptance
# state machine. Re-exported bare so test_live_acceptance.py's four direct
# calls keep resolving; its own call site in _run_live_acceptance below is
# qualified instead, since the function is ALSO a monkeypatch target
# (test_live_acceptance.py, 2 sites) and a bare reference here would not
# observe a patch applied to the new module after import.
_verify_secure_runtime_acceptance = (
    live_acceptance_secure_runtime._verify_secure_runtime_acceptance  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_packaged_mcp.py
_packaged_mcp_acceptance_evidence = (
    live_acceptance_packaged_mcp._packaged_mcp_acceptance_evidence  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validation_check = (
    live_acceptance_packaged_mcp._validation_check  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_handoff.py
_select_secure_runtime_handoff = (
    live_acceptance_handoff._select_secure_runtime_handoff  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validated_secure_runtime_pending_bind = (
    live_acceptance_handoff._validated_secure_runtime_pending_bind  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_browser_evidence.py -- _browser_json_observation/
# _browser_sse_observation are also test_live_acceptance.py monkeypatch
# targets, repointed at their real owner (not through this bare re-export).
_browser_json_observation = (
    live_acceptance_browser_evidence._browser_json_observation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_browser_sse_observation = (
    live_acceptance_browser_evidence._browser_sse_observation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_agent_prompt.py
_write_generated_agent_prompt = (
    live_acceptance_agent_prompt._write_generated_agent_prompt  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_job_verification.py -- _verify_completed_job IS a
# monkeypatch target (test_live_acceptance.py) with two still-resident
# callers in _run_live_acceptance below, so its call sites are qualified
# rather than routed through a bare re-export.
_find_agent_child_job = (
    live_acceptance_job_verification._find_agent_child_job  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_wait.py -- _wait_for_success IS a monkeypatch target
# (test_live_acceptance.py) with three still-resident callers in
# _run_live_acceptance below, so its call sites are qualified rather than
# routed through a bare re-export.
_require_secure_runtime_control_capacity = (
    live_acceptance_wait._require_secure_runtime_control_capacity  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_live_package_progress = (
    live_acceptance_wait._verify_live_package_progress  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_wait_for_live_structured_runtime_metadata = (
    live_acceptance_wait._wait_for_live_structured_runtime_metadata  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_checkpoint.py
_live_acceptance_intent_sha256 = (
    live_acceptance_checkpoint._live_acceptance_intent_sha256  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_live_acceptance_pending = (
    live_acceptance_checkpoint._live_acceptance_pending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_load_live_acceptance_resume = (
    live_acceptance_checkpoint._load_live_acceptance_resume  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_record_live_acceptance_pending = (
    live_acceptance_checkpoint._record_live_acceptance_pending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_resumed_live_acceptance_report = (
    live_acceptance_checkpoint._resumed_live_acceptance_report  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_progress.py
_assert_progress_adapter = (
    live_acceptance_progress._assert_progress_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_expected_progress_adapter = (
    live_acceptance_progress._expected_progress_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_expected_progress_package = (
    live_acceptance_progress._expected_progress_package  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_native_progress_attestation = (
    live_acceptance_progress._native_progress_attestation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_progress_attestation_identity = (
    live_acceptance_progress._progress_attestation_identity  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_runtime_metadata_from_job_status = (
    live_acceptance_progress._runtime_metadata_from_job_status  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_secure_runtime_probe_config = (
    live_acceptance_progress._secure_runtime_probe_config  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_progress_monitor = (
    live_acceptance_progress._verify_progress_monitor  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_runtime_metadata_artifact = (
    live_acceptance_progress._verify_runtime_metadata_artifact  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


def run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner | None = None,
) -> list[str]:
    """Run live checks and persist a report even when acceptance fails."""
    command_runner = runner or _run_command
    resume_report: LiveValidationReport | None = None
    resume_checkpoint: LiveAcceptanceCheckpoint | None = None
    if options.resume_report_path is not None:
        if options.report_path is None:
            raise ConfigurationError("resuming live acceptance requires a new report path")
        if options.report_path.resolve() == options.resume_report_path.resolve():
            raise ConfigurationError(
                "--report must differ from --resume-report so the checkpoint is preserved"
            )
        resume_report, resume_checkpoint = _load_live_acceptance_resume(options)
    recorder: ValidationRecorder | None = None
    if options.report_path is not None:
        transport_modes: list[str] = []
        verify_transport = (
            options.definition.live_test.verify_transport
            if options.verify_transport is None
            else options.verify_transport
        )
        verify_direct = (
            options.definition.live_test.verify_direct_transport
            if options.verify_direct_transport is None
            else options.verify_direct_transport
        )
        if verify_transport:
            transport_modes.append("frp-relay")
        if verify_direct:
            transport_modes.append("frp-direct")
        if options.verify_ssh_transport:
            transport_modes.append("ssh-forward")
        report = (
            _resumed_live_acceptance_report(
                resume_report,
                report_id=options.report_id,
            )
            if resume_report is not None
            else new_live_validation_report(
                scenario=options.validation_scenario,
                cluster=options.cluster,
                transport_modes=transport_modes,
                launcher=options.validation_launcher,
                install_source=options.validation_install_source,
                artifact_sha256=options.validation_artifact_sha256,
                report_id=options.report_id,
            )
        )
        recorder = ValidationRecorder(report)
        if transport_modes:
            recorder.report.cleanup = CleanupEvidence(
                requested=True,
                mode="transport_probe_teardown",
                cancel_scheduler_jobs=False,
            )
    try:
        lines = _run_live_acceptance(
            options,
            runner=command_runner,
            recorder=recorder,
            resume_checkpoint=resume_checkpoint,
        )
    except _LiveAcceptancePending as pending:
        if recorder is None or options.report_path is None:
            raise ConfigurationError(
                "a pending live acceptance observation requires a machine-readable report"
            ) from pending
        _record_live_acceptance_pending(recorder, pending)
        recorder.write(options.report_path, options.markdown_report_path)
        return [
            "validation.status=pending",
            f"acceptance.run_id={pending.checkpoint.run_id}",
            f"acceptance.job_id={pending.checkpoint.primary_job_id}",
            f"acceptance.pending_phase={pending.checkpoint.phase}",
            "acceptance.scheduler_action=none",
            "acceptance.relay_action=observe_existing",
            f"validation.report={options.report_path.resolve()}",
        ]
    except BaseException as exc:
        if recorder is not None:
            for evidence_line in transport_evidence_lines_from_error(exc):
                try:
                    recorder.observe_line(evidence_line)
                except Exception as evidence_error:
                    recorder.record_failure(
                        "transport.structured-evidence",
                        "ingest structured transport cleanup evidence",
                        evidence_error,
                    )
            recorder.record_failure(
                "live-test.completed", "complete configured live acceptance", exc
            )
            recorder.finish(exc)
            assert options.report_path is not None
            recorder.write(options.report_path, options.markdown_report_path)
        raise
    if recorder is not None:
        recorder.finish()
        assert options.report_path is not None
        recorder.write(options.report_path, options.markdown_report_path)
        lines.append(f"validation.report={options.report_path.resolve()}")
    return lines


def _run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner,
    recorder: ValidationRecorder | None,
    resume_checkpoint: LiveAcceptanceCheckpoint | None = None,
) -> list[str]:
    """Execute the acceptance workflow while emitting structured facts."""
    command_runner = runner
    jarvis_yaml = options.jarvis_yaml or _configured_path(options.definition.live_test.jarvis_yaml)
    monitor_pattern = options.monitor_pattern or options.definition.live_test.monitor_pattern
    progress_pattern = options.progress_pattern or options.definition.live_test.progress_pattern
    progress_action_payload = (
        options.progress_action_payload
        if options.progress_action_payload
        else options.definition.live_test.progress_action_payload
    )
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    agent_child_jarvis_yaml = options.agent_child_jarvis_yaml or _configured_path(
        options.definition.live_test.agent_child_jarvis_yaml
    )
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
    verify_transport = (
        options.definition.live_test.verify_transport
        if options.verify_transport is None
        else options.verify_transport
    )
    if jarvis_yaml is None:
        raise ConfigurationError(
            "live-test requires --jarvis-yaml or cluster live_test.jarvis_yaml"
        )
    if not jarvis_yaml.exists():
        raise ConfigurationError(f"live-test JARVIS YAML does not exist: {jarvis_yaml}")
    if agent_child_jarvis_yaml is not None and not agent_child_jarvis_yaml.exists():
        raise ConfigurationError(
            f"live-test agent child JARVIS YAML does not exist: {agent_child_jarvis_yaml}"
        )
    if agent_child_jarvis_yaml is not None and agent_mcp_config is None:
        raise ConfigurationError(
            "live-test --agent-child-jarvis-yaml requires --agent-mcp-config "
            "or cluster live_test.agent_mcp_config"
        )
    if agent_child_jarvis_yaml is not None and agent_prompt is not None:
        raise ConfigurationError(
            "live-test cannot use both an explicit agent prompt and agent_child_jarvis_yaml"
        )
    transport_token: str | None = None
    transport_secret_key: str | None = None
    verify_direct_transport = (
        options.definition.live_test.verify_direct_transport
        if options.verify_direct_transport is None
        else options.verify_direct_transport
    )
    allow_direct_transport_fallback = (
        options.definition.live_test.allow_direct_transport_fallback
        if options.allow_direct_transport_fallback is None
        else options.allow_direct_transport_fallback
    )
    if verify_transport or verify_direct_transport:
        transport_token, transport_secret_key = _require_transport_secrets(
            token=options.transport_token,
            secret_key=options.transport_secret_key,
        )
    source_pipeline_yaml = jarvis_yaml.read_text(encoding="utf-8")
    pipeline_sha256 = hashlib.sha256(source_pipeline_yaml.encode("utf-8")).hexdigest()
    intent_sha256 = _live_acceptance_intent_sha256(
        options,
        jarvis_yaml=jarvis_yaml,
        pipeline_sha256=pipeline_sha256,
        monitor_pattern=monitor_pattern,
        progress_pattern=progress_pattern,
        progress_action_payload=progress_action_payload,
        agent_prompt=agent_prompt,
        agent_mcp_config=agent_mcp_config,
        agent_child_jarvis_yaml=agent_child_jarvis_yaml,
        require_agent_child_job=require_agent_child_job,
        verify_transport=verify_transport,
        verify_direct_transport=verify_direct_transport,
        allow_direct_transport_fallback=allow_direct_transport_fallback,
    )
    if resume_checkpoint is None:
        run_id = _acceptance_run_id(jarvis_yaml)
        remote_yaml = f".local/share/clio-relay/live-tests/{run_id}/pipeline.yaml"
        state = _LiveAcceptanceState(
            run_id=run_id,
            intent_sha256=intent_sha256,
            pipeline_sha256=pipeline_sha256,
            remote_pipeline_path=remote_yaml,
            primary_idempotency_key=f"live-test:{options.cluster}:{run_id}:jarvis",
            agent_prompt=agent_prompt,
        )
    else:
        state = _LiveAcceptanceState.from_checkpoint(resume_checkpoint)
        run_id = state.run_id
        remote_yaml = state.remote_pipeline_path
        agent_prompt = state.agent_prompt
    secure_runtime_probe = _secure_runtime_probe_config(source_pipeline_yaml)
    pipeline_yaml_text = _stage_acceptance_files(
        options.definition,
        jarvis_yaml=jarvis_yaml,
        pipeline_yaml_text=source_pipeline_yaml,
        run_id=run_id,
        runner=command_runner,
        write_remote=resume_checkpoint is None,
    )
    expected_progress_adapter = _expected_progress_adapter(pipeline_yaml_text)
    expected_progress_package = _expected_progress_package(pipeline_yaml_text)
    lines: list[str] = _ValidationLines(recorder)
    if expected_progress_adapter is not None:
        if expected_progress_package is None:
            raise ConfigurationError(
                "an explicit package progress adapter requires exactly one non-empty pkg_type"
            )
        lines.append("acceptance.application_boundary=package_progress_provider")
        lines.append(f"acceptance.package_adapter={expected_progress_adapter}")
        lines.append(f"acceptance.package_owner={expected_progress_package}")

    if resume_checkpoint is None:
        lines.extend(run_cluster_doctor(options.definition))
        lines.append("acceptance.cluster_doctor=passed")
    else:
        lines.append(f"acceptance.resume_run_id={run_id}")
        lines.append(f"acceptance.resume_phase={resume_checkpoint.phase}")
    if options.verify_cluster_deployment and resume_checkpoint is None:
        lines.extend(
            _verify_cluster_deployment(
                options.definition,
                runner=command_runner,
                expected_artifact_sha256=options.validation_artifact_sha256,
                expected_install_source=(
                    recorder.report.install_source.kind.value if recorder is not None else None
                ),
            )
        )
    if secure_runtime_probe is not None and resume_checkpoint is None:
        if recorder is None:
            raise ConfigurationError(
                "secure runtime acceptance requires a machine-readable report path"
            )
        with _validation_check(
            recorder,
            "secure-runtime.control-query-capacity",
            "verify one free reserved control-query slot before source submission",
            forbidden_values=set(),
        ) as evidence:
            _require_secure_runtime_control_capacity(
                options.definition,
                cluster=options.cluster,
                runner=command_runner,
                evidence=evidence,
            )
        lines.append("secure-runtime.control_query_capacity=ready")
    if verify_transport and resume_checkpoint is None:
        assert transport_token is not None
        assert transport_secret_key is not None
        lines.extend(
            _verify_transport(
                options,
                token=transport_token,
                secret_key=transport_secret_key,
                pipeline_yaml=pipeline_yaml_text,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
            )
        )
    if verify_direct_transport and resume_checkpoint is None:
        assert transport_token is not None
        assert transport_secret_key is not None
        direct_lines = _verify_direct_transport(
            options,
            token=transport_token,
            secret_key=transport_secret_key,
            allow_stcp_fallback=allow_direct_transport_fallback,
            pipeline_yaml=pipeline_yaml_text,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        )
        if not allow_direct_transport_fallback:
            _assert_direct_xtcp_acceptance(direct_lines)
        lines.extend(direct_lines)
    if options.verify_ssh_transport and resume_checkpoint is None:
        lines.extend(_verify_ssh_transport(options, pipeline_yaml=pipeline_yaml_text))
    if resume_checkpoint is None:
        _remote_write_file(
            options.definition.ssh_host,
            remote_yaml,
            pipeline_yaml_text.encode("utf-8"),
            runner=command_runner,
        )
    lines.append(f"acceptance.pipeline={remote_yaml}")
    if agent_child_jarvis_yaml is not None and resume_checkpoint is None:
        agent_prompt = _write_generated_agent_prompt(
            options.definition,
            cluster=options.cluster,
            run_id=run_id,
            child_yaml=agent_child_jarvis_yaml,
            runner=command_runner,
        )
        state.agent_prompt = agent_prompt
        lines.append(f"acceptance.agent_prompt={agent_prompt}")
    elif agent_prompt is not None:
        lines.append(f"acceptance.agent_prompt={agent_prompt}")

    if resume_checkpoint is None:
        submit = _remote_clio_json(
            options.definition,
            [
                "job",
                "submit",
                "--cluster",
                options.cluster,
                "--jarvis-yaml",
                remote_yaml,
                "--idempotency-key",
                state.primary_idempotency_key,
            ],
            runner=command_runner,
            raw_text=True,
        )
        job_id = submit.strip().splitlines()[-1]
        if not job_id.startswith("job_"):
            raise RelayError(f"live-test submit did not return a job id: {submit}")
        state.primary_job_id = job_id
    else:
        assert state.primary_job_id is not None
        job_id = state.primary_job_id
    lines.append(f"acceptance.job_id={job_id}")

    resume_phase = resume_checkpoint.phase if resume_checkpoint is not None else None
    post_primary_phases = {
        "secure_runtime_metadata",
        "secure_runtime_query",
        "secure_runtime_bind",
        "agent_job_wait",
        "agent_child_job_wait",
    }

    if expected_progress_adapter is not None and resume_checkpoint is None:
        _verify_live_package_progress(
            options.definition,
            job_id,
            expected_progress_adapter,
            package_name=expected_progress_package,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append(f"acceptance.live_progress_adapter={expected_progress_adapter}")

    secure_runtime_forbidden_values: set[str] = set()
    if secure_runtime_probe is None:
        if resume_phase not in post_primary_phases:
            try:
                live_acceptance_wait._wait_for_success(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    options.definition,
                    job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="primary_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            lines.append("acceptance.job_state=succeeded")
            if options.verify_cluster_deployment:
                lines.append("worker.execute=passed")

            live_acceptance_job_verification._verify_completed_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                options.definition,
                job_id,
                line_prefix="acceptance",
                lines=lines,
                runner=command_runner,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
                recorder=recorder,
                require_structured_runtime_metadata=options.require_structured_runtime_metadata,
            )
    else:
        assert recorder is not None
        if resume_phase in {"secure_runtime_query", "secure_runtime_bind"}:
            assert state.pipeline_id is not None and state.execution_id is not None
            runtime_document = {
                "pipeline_id": state.pipeline_id,
                "execution_id": state.execution_id,
            }
        else:
            try:
                with _validation_check(
                    recorder,
                    "secure-runtime.source-live-metadata",
                    "observe trusted runtime metadata while retaining the running source job",
                    forbidden_values=set(),
                ) as evidence:
                    runtime_metadata = _wait_for_live_structured_runtime_metadata(
                        options.definition,
                        job_id,
                        line_prefix="acceptance",
                        lines=lines,
                        timeout_seconds=options.timeout_seconds,
                        poll_seconds=options.poll_seconds,
                        runner=command_runner,
                    )
                    runtime_document = runtime_metadata.document
                    runtime_source = str(runtime_document["source"])
                    evidence.append(
                        EvidenceReference(
                            kind="relay_job_status",
                            reference=f"relay-job://{options.cluster}/{job_id}",
                            metadata={
                                "state": JobState.RUNNING.value,
                                "runtime_metadata_source": runtime_source,
                                "source_job_retained": True,
                                "cancel_scheduler_job": False,
                            },
                        )
                    )
                    recorder.add_resource(
                        ValidationResource(
                            kind="relay_job",
                            resource_id=job_id,
                            role="secure_runtime_source",
                            cluster=options.cluster,
                            state=JobState.RUNNING.value,
                            metadata={
                                "runtime_metadata_source": runtime_source,
                                "retained": True,
                                "cancel_scheduler_job": False,
                            },
                        )
                    )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            state.pipeline_id = cast(str, runtime_document["pipeline_id"])
            state.execution_id = cast(str, runtime_document["execution_id"])
        try:
            secure_runtime_forbidden_values = (
                live_acceptance_secure_runtime._verify_secure_runtime_acceptance(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                    options,
                    config=secure_runtime_probe,
                    runtime_metadata=runtime_document,
                    recorder=recorder,
                )
            )
        except _AcceptanceObservationPending as pending:
            raise _live_acceptance_pending(
                options,
                state=state,
                recorder=recorder,
                pending=pending,
            ) from None
        lines.append("secure-runtime.acceptance=ok")

    resuming_agent_phase = resume_phase in {"agent_job_wait", "agent_child_job_wait"}
    if monitor_pattern is not None and not resuming_agent_phase:
        _remote_clio_json(
            options.definition,
            [
                "monitor",
                "add-regex",
                job_id,
                "--pattern",
                monitor_pattern,
                "--event-type",
                "stdout.delta",
            ],
            runner=command_runner,
        )
        actions = _remote_clio_json(
            options.definition,
            ["monitor", "run-once", "--limit", "250"],
            runner=command_runner,
        )
        if not actions:
            raise RelayError(f"acceptance monitor pattern did not match: {monitor_pattern}")
        lines.append("acceptance.monitor=ok")

    if progress_pattern is not None and not resuming_agent_phase:
        _verify_progress_monitor(
            options.definition,
            job_id,
            pattern=progress_pattern,
            action_payload=progress_action_payload,
            lines=lines,
            runner=command_runner,
        )

    if agent_prompt is not None:
        if resuming_agent_phase:
            assert state.agent_job_id is not None
            agent_job_id = state.agent_job_id
        else:
            agent_args = [
                "agent",
                "run",
                "--cluster",
                options.cluster,
                "--prompt",
                agent_prompt,
                "--idempotency-key",
                f"live-test:{options.cluster}:{run_id}:agent",
            ]
            if agent_mcp_config is not None:
                agent_args.extend(["--mcp-config", agent_mcp_config])
            agent_submit = _remote_clio_json(
                options.definition,
                agent_args,
                runner=command_runner,
                raw_text=True,
            )
            agent_job_id = agent_submit.strip().splitlines()[-1]
            if not agent_job_id.startswith("job_"):
                raise RelayError(f"live-test agent submit did not return a job id: {agent_submit}")
            state.agent_job_id = agent_job_id
        if resume_phase != "agent_child_job_wait":
            try:
                agent_job = live_acceptance_wait._wait_for_success(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    options.definition,
                    agent_job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="agent_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
        else:
            agent_job = {}
        lines.append(f"acceptance.agent_job_id={agent_job_id}")
        lines.append("acceptance.agent_state=succeeded")
        if require_agent_child_job:
            if resume_phase == "agent_child_job_wait":
                assert state.agent_child_job_id is not None
                child_job_id = state.agent_child_job_id
            else:
                child_job_id = _find_agent_child_job(
                    options.definition,
                    agent_job_id,
                    agent_created_at=str(agent_job["created_at"]),
                    runner=command_runner,
                )
                state.agent_child_job_id = child_job_id
            try:
                live_acceptance_wait._wait_for_success(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    options.definition,
                    child_job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="agent_child_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            lines.append(f"acceptance.agent_child_job_id={child_job_id}")
            live_acceptance_job_verification._verify_completed_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                options.definition,
                child_job_id,
                line_prefix="acceptance.agent_child",
                lines=lines,
                runner=command_runner,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
                recorder=recorder,
                require_structured_runtime_metadata=options.require_structured_runtime_metadata,
            )

    lines.append("live acceptance passed")
    expected_transport_cleanups = (
        0
        if resume_checkpoint is not None
        else sum([verify_transport, verify_direct_transport, options.verify_ssh_transport])
    )
    observed_transport_cleanups = lines.count("transport.cleanup=passed")
    if observed_transport_cleanups < expected_transport_cleanups:
        raise RelayError(
            "transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={observed_transport_cleanups}"
        )
    if recorder is not None and recorder.transport_probe_count < expected_transport_cleanups:
        raise RelayError(
            "structured transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={recorder.transport_probe_count}"
        )
    if recorder is not None and recorder.report.cleanup.remaining_resources:
        raise RelayError(
            "transport cleanup left structured residual resources: "
            f"count={len(recorder.report.cleanup.remaining_resources)}"
        )
    if recorder is not None and secure_runtime_probe is not None:
        _assert_secret_free_document(
            recorder.report.model_dump(mode="json"),
            forbidden_values=secure_runtime_forbidden_values,
            label="secure runtime validation report",
        )
    return lines
