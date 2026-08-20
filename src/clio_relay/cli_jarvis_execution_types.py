"""JARVIS execution-query acceptance types (iowarp/clio-relay#231
continuation): the three dataclasses describing an execution-query's
acceptance, pending, and single-attempt shapes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from clio_relay.errors import RelayError
from clio_relay.mcp_stdio_validation import PackagedMcpStdioSession


@dataclass(frozen=True)
class _JarvisExecutionQueryAcceptance:
    """Durable evidence from one post-run unified JARVIS execution query."""

    cluster: str
    pipeline_id: str
    execution_id: str
    outcome: Literal["terminal", "observation_unknown", "terminal_artifacts_pending"]
    tools_list_response: dict[str, Any]
    call_response: dict[str, Any]
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
    initialize_response: dict[str, Any]
    stdio_evidence: dict[str, Any]
    lifecycle_observations: list[dict[str, Any]]
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["none"] = "none"

    def retry_selector(self) -> dict[str, object]:
        """Return the exact execution identity for a later query-only observation."""
        if not self.lifecycle_observations:
            raise RelayError("JARVIS execution observation omitted durable lifecycle evidence")
        latest = self.lifecycle_observations[-1]
        handle = latest.get("execution_handle")
        if not isinstance(handle, dict):
            raise RelayError("JARVIS execution observation omitted its durable handle")
        typed_handle = cast(dict[str, object], handle)
        scheduler_cluster = typed_handle.get("cluster")
        if scheduler_cluster is not None and (
            not isinstance(scheduler_cluster, str) or not scheduler_cluster
        ):
            raise RelayError("JARVIS execution observation returned an invalid scheduler cluster")
        return {
            "cluster": self.cluster,
            "scheduler_cluster": scheduler_cluster,
            "pipeline_id": self.pipeline_id,
            "execution_id": self.execution_id,
            "scheduler_provider": typed_handle.get("scheduler_provider"),
            "scheduler_native_id": typed_handle.get("scheduler_native_id"),
            "last_query_job_id": self.call_job_id,
        }


@dataclass(frozen=True)
class _JarvisExecutionQueryPending:
    """Exact query-only resume identity before the first execution snapshot arrives."""

    cluster: str
    pipeline_id: str
    execution_id: str
    selector: dict[str, object]
    outcome: Literal["observation_pending"] = "observation_pending"
    lifecycle_observations: tuple[()] = ()
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["retain"] = "retain"

    def retry_selector(self) -> dict[str, object]:
        """Return the exact execution identity without inventing query evidence."""
        if (
            self.selector.get("cluster") != self.cluster
            or self.selector.get("pipeline_id") != self.pipeline_id
            or self.selector.get("execution_id") != self.execution_id
            or self.selector.get("last_query_job_id") is not None
        ):
            raise RelayError("unobserved JARVIS execution selector is inconsistent")
        return dict(self.selector)


@dataclass(frozen=True)
class _JarvisExecutionQueryAttempt:
    """One durable ``jarvis_get_execution`` query and its local transport evidence."""

    session: PackagedMcpStdioSession
    call_job_id: str
    call_status: dict[str, Any]
    artifacts: list[dict[str, Any]]
    mcp_result: dict[str, Any] | None
    provenance: dict[str, Any] | None
