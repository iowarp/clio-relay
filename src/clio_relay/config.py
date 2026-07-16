"""Configuration loading for clio-relay."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.spool import (
    DEFAULT_MAX_LOG_BYTES_PER_JOB,
    DEFAULT_MAX_LOG_BYTES_PER_STREAM,
)
from clio_relay.storage_policy import (
    DEFAULT_JOB_CORE_ALLOWANCE_BYTES,
    DEFAULT_JOB_RESULT_ALLOWANCE_BYTES,
    DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS,
    StorageLimits,
)

_DEFAULT_STORAGE_LIMITS = StorageLimits()


class RelaySettings(BaseModel):
    """Runtime settings loaded from environment variables."""

    model_config = ConfigDict(extra="forbid")

    core_dir: Path = Field(default_factory=lambda: Path(".clio-relay/core"))
    spool_dir: Path = Field(default_factory=lambda: Path(".clio-relay/spool"))
    spool_max_log_bytes_per_stream: int = Field(
        default=DEFAULT_MAX_LOG_BYTES_PER_STREAM,
        ge=1,
    )
    spool_max_log_bytes_per_job: int = Field(
        default=DEFAULT_MAX_LOG_BYTES_PER_JOB,
        ge=1,
    )
    storage_core_high_water_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.core_high_water_bytes,
        ge=1,
    )
    storage_spool_high_water_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.spool_high_water_bytes,
        ge=1,
    )
    storage_total_high_water_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.total_high_water_bytes,
        ge=1,
    )
    storage_minimum_free_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.minimum_free_bytes,
        ge=0,
    )
    storage_max_job_reservation_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_job_reservation_bytes,
        ge=1,
    )
    storage_max_scan_entries: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_scan_entries,
        ge=1,
    )
    storage_max_scan_depth: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_scan_depth,
        ge=1,
    )
    storage_max_scan_accounted_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_scan_accounted_bytes,
        ge=1,
    )
    storage_max_ledger_bytes: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_ledger_bytes,
        ge=1,
    )
    storage_max_reservations: int = Field(
        default=_DEFAULT_STORAGE_LIMITS.max_reservations,
        ge=1,
    )
    storage_lock_timeout_seconds: float = Field(
        default=_DEFAULT_STORAGE_LIMITS.lock_timeout_seconds,
        gt=0,
        le=300,
    )
    storage_job_core_allowance_bytes: int = Field(
        default=DEFAULT_JOB_CORE_ALLOWANCE_BYTES,
        ge=1,
    )
    storage_job_result_allowance_bytes: int = Field(
        default=DEFAULT_JOB_RESULT_ALLOWANCE_BYTES,
        ge=1,
    )
    storage_runtime_check_interval_seconds: float = Field(
        default=DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS,
        gt=0,
        le=300,
    )
    frps_addr: str | None = None
    frp_token: str | None = None
    jarvis_bin: str = "jarvis"
    frpc_bin: str = "frpc"
    api_token: str | None = None
    owner_session_id: str | None = None
    owner_session_generation_id: DurableRecordId | None = None
    remote_cluster: str | None = None
    session_owner_token: str | None = None
    agent_bin: str = "agent"
    agent_adapter: str = "exec"
    agent_args: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_owner_session_identity(self) -> Self:
        """Require session id and generation to appear as one authoritative identity."""
        if (self.owner_session_id is None) != (self.owner_session_generation_id is None):
            raise ValueError(
                "owner_session_id and owner_session_generation_id must be configured together"
            )
        return self

    @model_validator(mode="after")
    def validate_storage_policy(self) -> Self:
        """Validate limits and ensure default per-job sizing fits the configured cap."""
        self.storage_limits()
        default_total = (
            2 * self.spool_max_log_bytes_per_job
            + self.storage_job_core_allowance_bytes
            + self.storage_job_result_allowance_bytes
        )
        default_core = self.spool_max_log_bytes_per_job + self.storage_job_core_allowance_bytes
        default_spool = self.spool_max_log_bytes_per_job + self.storage_job_result_allowance_bytes
        if default_total > self.storage_max_job_reservation_bytes:
            raise ValueError(
                "default storage reservation exceeds storage_max_job_reservation_bytes"
            )
        if default_core > self.storage_core_high_water_bytes:
            raise ValueError("default core reservation exceeds storage core high-water limit")
        if default_spool > self.storage_spool_high_water_bytes:
            raise ValueError("default spool reservation exceeds storage spool high-water limit")
        if default_total > self.storage_total_high_water_bytes:
            raise ValueError("default storage reservation exceeds storage total high-water limit")
        return self

    def storage_limits(self) -> StorageLimits:
        """Build the validated queue and spool storage safety limits."""
        return StorageLimits(
            core_high_water_bytes=self.storage_core_high_water_bytes,
            spool_high_water_bytes=self.storage_spool_high_water_bytes,
            total_high_water_bytes=self.storage_total_high_water_bytes,
            minimum_free_bytes=self.storage_minimum_free_bytes,
            max_job_reservation_bytes=self.storage_max_job_reservation_bytes,
            max_scan_entries=self.storage_max_scan_entries,
            max_scan_depth=self.storage_max_scan_depth,
            max_scan_accounted_bytes=self.storage_max_scan_accounted_bytes,
            max_ledger_bytes=self.storage_max_ledger_bytes,
            max_reservations=self.storage_max_reservations,
            lock_timeout_seconds=self.storage_lock_timeout_seconds,
        )

    @classmethod
    def from_env(cls) -> RelaySettings:
        """Load settings from the current process environment."""
        return cls(
            core_dir=_env_or_bootstrap_data_dir("CLIO_RELAY_CORE_DIR", "core"),
            spool_dir=_env_or_bootstrap_data_dir("CLIO_RELAY_SPOOL_DIR", "spool"),
            spool_max_log_bytes_per_stream=_positive_int_env(
                "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_STREAM",
                DEFAULT_MAX_LOG_BYTES_PER_STREAM,
            ),
            spool_max_log_bytes_per_job=_positive_int_env(
                "CLIO_RELAY_SPOOL_MAX_LOG_BYTES_PER_JOB",
                DEFAULT_MAX_LOG_BYTES_PER_JOB,
            ),
            storage_core_high_water_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_CORE_HIGH_WATER_BYTES",
                _DEFAULT_STORAGE_LIMITS.core_high_water_bytes,
            ),
            storage_spool_high_water_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_SPOOL_HIGH_WATER_BYTES",
                _DEFAULT_STORAGE_LIMITS.spool_high_water_bytes,
            ),
            storage_total_high_water_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_TOTAL_HIGH_WATER_BYTES",
                _DEFAULT_STORAGE_LIMITS.total_high_water_bytes,
            ),
            storage_minimum_free_bytes=_nonnegative_int_env(
                "CLIO_RELAY_STORAGE_MINIMUM_FREE_BYTES",
                _DEFAULT_STORAGE_LIMITS.minimum_free_bytes,
            ),
            storage_max_job_reservation_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_JOB_RESERVATION_BYTES",
                _DEFAULT_STORAGE_LIMITS.max_job_reservation_bytes,
            ),
            storage_max_scan_entries=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_SCAN_ENTRIES",
                _DEFAULT_STORAGE_LIMITS.max_scan_entries,
            ),
            storage_max_scan_depth=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_SCAN_DEPTH",
                _DEFAULT_STORAGE_LIMITS.max_scan_depth,
            ),
            storage_max_scan_accounted_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_SCAN_ACCOUNTED_BYTES",
                _DEFAULT_STORAGE_LIMITS.max_scan_accounted_bytes,
            ),
            storage_max_ledger_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_LEDGER_BYTES",
                _DEFAULT_STORAGE_LIMITS.max_ledger_bytes,
            ),
            storage_max_reservations=_positive_int_env(
                "CLIO_RELAY_STORAGE_MAX_RESERVATIONS",
                _DEFAULT_STORAGE_LIMITS.max_reservations,
            ),
            storage_lock_timeout_seconds=_positive_float_env(
                "CLIO_RELAY_STORAGE_LOCK_TIMEOUT_SECONDS",
                _DEFAULT_STORAGE_LIMITS.lock_timeout_seconds,
            ),
            storage_job_core_allowance_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_JOB_CORE_ALLOWANCE_BYTES",
                DEFAULT_JOB_CORE_ALLOWANCE_BYTES,
            ),
            storage_job_result_allowance_bytes=_positive_int_env(
                "CLIO_RELAY_STORAGE_JOB_RESULT_ALLOWANCE_BYTES",
                DEFAULT_JOB_RESULT_ALLOWANCE_BYTES,
            ),
            storage_runtime_check_interval_seconds=_positive_float_env(
                "CLIO_RELAY_STORAGE_RUNTIME_CHECK_INTERVAL_SECONDS",
                DEFAULT_RUNTIME_CHECK_INTERVAL_SECONDS,
            ),
            frps_addr=os.getenv("CLIO_RELAY_FRPS_ADDR"),
            frp_token=os.getenv("CLIO_RELAY_FRP_TOKEN"),
            jarvis_bin=_env_or_bootstrap_bin("CLIO_RELAY_JARVIS_BIN", "jarvis"),
            frpc_bin=_env_or_bootstrap_bin("CLIO_RELAY_FRPC_BIN", "frpc"),
            api_token=os.getenv("CLIO_RELAY_API_TOKEN"),
            owner_session_id=os.getenv("CLIO_RELAY_OWNER_SESSION_ID"),
            owner_session_generation_id=os.getenv("CLIO_RELAY_SESSION_GENERATION_ID"),
            remote_cluster=os.getenv("CLIO_RELAY_REMOTE_CLUSTER"),
            session_owner_token=os.getenv("CLIO_RELAY_SESSION_OWNER_TOKEN"),
            agent_bin=os.getenv(
                "CLIO_RELAY_AGENT_BIN",
                "agent",
            ),
            agent_adapter=os.getenv("CLIO_RELAY_AGENT_ADAPTER", "exec"),
            agent_args=_split_args(os.getenv("CLIO_RELAY_AGENT_ARGS")),
        )


def _env_or_bootstrap_data_dir(env_name: str, family: str) -> Path:
    configured = os.getenv(env_name)
    if configured:
        return Path(configured).expanduser().resolve()
    bootstrap_path = Path.home() / ".local" / "share" / "clio-relay" / family
    if bootstrap_path.exists():
        return bootstrap_path.resolve()
    return Path(".clio-relay") / family


def _env_or_bootstrap_bin(env_name: str, executable_name: str) -> str:
    configured = os.getenv(env_name)
    if configured:
        return configured
    path_executable = shutil.which(executable_name)
    if path_executable is not None and _candidate_is_usable(executable_name, Path(path_executable)):
        return executable_name
    bootstrap_path = Path.home() / ".local" / "bin" / executable_name
    if bootstrap_path.exists() and _candidate_is_usable(executable_name, bootstrap_path):
        return str(bootstrap_path)
    local_tool_path = _local_tool_bin(executable_name)
    if local_tool_path is not None and _candidate_is_usable(executable_name, local_tool_path):
        return str(local_tool_path)
    return executable_name


def _candidate_is_usable(executable_name: str, path: Path) -> bool:
    if executable_name not in {"frpc", "frps"}:
        return True
    try:
        result = subprocess.run(
            [str(path), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except OSError:
        return False
    except subprocess.TimeoutExpired:
        return False
    return result.returncode == 0


def _local_tool_bin(executable_name: str) -> Path | None:
    candidates = [Path.cwd() / ".tools" / "frp" / "bin" / executable_name]
    if os.name == "nt" and not executable_name.lower().endswith(".exe"):
        candidates.append(Path.cwd() / ".tools" / "frp" / "bin" / f"{executable_name}.exe")
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def _split_args(value: str | None) -> list[str]:
    if value is None or value.strip() == "":
        return []
    return shlex.split(value)


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a non-negative integer") from exc
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not 0 < value <= 300:
        raise ValueError(f"{name} must be greater than zero and at most 300")
    return value
