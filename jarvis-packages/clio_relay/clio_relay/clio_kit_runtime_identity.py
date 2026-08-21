"""Verify a locked clio-kit child launcher, from either a wheel or an installed tool.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).

``_installed_clio_kit_runtime_identity`` and ``_locked_clio_kit_runtime_identity``
call ``_file_identity``/``_resolve_executable`` -- both individually monkeypatched
by ``tests/test_mcp_call_runner.py`` on the ``runner`` facade module and expected
to take effect here. Since this module's own top-level import binds those names
at import time (immune to a later ``monkeypatch.setattr(runner, ...)``), both
calls go through ``_facade()`` -- a deferred, call-time attribute lookup on the
``clio_relay.mcp_call.runner`` module object -- instead. See the ``runner.py``
module docstring for the full reach-back contract this decomposition wave relies
on.
"""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from typing import Any, cast

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.bounded_file_io import bounded_regular_file_bytes, is_sha256_text
from clio_relay.clio_kit_wheel_archive import (
    CLIO_KIT_WHEEL_MAX_PROJECT_BYTES,
    _zip_member_is_regular,
    bounded_zip_member_chunks,
    clio_kit_runtime_project_members,
    read_bounded_zip_member,
    validated_wheel_members,
    verified_wheel_archive,
)
from clio_relay.constants import (
    _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY,
    _CLIO_KIT_LOCKED_SERVER_SCHEMA,
    CLIO_KIT_LOCK_MAX_BYTES,
    CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
)
from clio_relay.jarvis_cd_lock_binding import _jarvis_cd_lock_binding


def _nested_clio_kit_server_name(
    server_args: list[str],
    *,
    python_distribution_runtime: dict[str, Any] | None,
) -> str | None:
    """Return the embedded server selected through clio-kit's child launcher."""
    for index, argument in enumerate(server_args[:-1]):
        if argument != "--from":
            continue
        command = server_args[index + 2 :]
        if (
            len(command) >= 3
            and command[0] == "clio-kit"
            and command[1] == "mcp-server"
            and command[2]
        ):
            return command[2]
        return None
    if (
        len(server_args) >= 2
        and server_args[0] == "mcp-server"
        and bool(server_args[1])
        and python_distribution_runtime is not None
        and str(python_distribution_runtime.get("distribution", "")).lower().replace("_", "-")
        == "clio-kit"
        and python_distribution_runtime.get("entry_point") == "clio-kit"
        and python_distribution_runtime.get("runtime_closure_verified") is True
    ):
        return server_args[1]
    return None


def _installed_clio_kit_runtime_identity(
    distribution_runtime: dict[str, Any] | None,
    *,
    server_name: str,
    resolved_executable: Path,
    verify_relay_jarvis_cd_lock: bool,
) -> dict[str, Any]:
    """Verify clio-kit's locked child launcher from a persistent tool environment."""
    uv_identity = _facade()._file_identity(Path(_facade()._resolve_executable("uv")).expanduser())
    evidence: dict[str, Any] = {
        "schema_version": _CLIO_KIT_LOCKED_SERVER_SCHEMA,
        "server_name": server_name,
        "runtime_policy": _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY,
        "project_sha256": None,
        "lock_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "contract_source_verified": False,
        "uv_executable": uv_identity,
        "persistent_tool": True,
        "locked_runtime_verified": False,
        "error": None,
    }
    if (
        distribution_runtime is None
        or str(distribution_runtime.get("distribution", "")).lower().replace("_", "-") != "clio-kit"
        or distribution_runtime.get("entry_point") != "clio-kit"
        or distribution_runtime.get("runtime_closure_verified") is not True
    ):
        evidence["error"] = "persistent clio-kit distribution closure is unverified"
        return evidence
    source_value = distribution_runtime.get("contract_source_path")
    lock_paths = distribution_runtime.get("server_lock_paths")
    lock_value = (
        cast(dict[str, Any], lock_paths).get(server_name) if isinstance(lock_paths, dict) else None
    )
    if not isinstance(source_value, str) or not isinstance(lock_value, str):
        evidence["error"] = "persistent clio-kit tool omitted launcher or server lock files"
        return evidence
    source_path = Path(source_value)
    lock_path = Path(lock_value)
    source = bounded_regular_file_bytes(
        source_path,
        max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
    )
    lock = bounded_regular_file_bytes(
        lock_path,
        max_bytes=CLIO_KIT_LOCK_MAX_BYTES,
    )
    lock_identity = _facade()._file_identity(lock_path)
    if source is None or lock is None or lock_identity is None:
        evidence["error"] = "persistent clio-kit launcher or lock file is unavailable"
        return evidence
    try:
        launcher_source = source.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        evidence["error"] = "persistent clio-kit launcher source is not UTF-8"
        return evidence
    contract_source_verified = all(
        marker in launcher_source
        for marker in (
            f'LOCKED_SERVER_LAUNCH_SCHEMA = "{_CLIO_KIT_LOCKED_SERVER_SCHEMA}"',
            f'_LOCKED_SERVER_RUNTIME_POLICY = "{_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY}"',
            '"--no-dev"',
            '"--no-editable"',
            '"--frozen"',
            "locked_server_project_identity",
            "materialize_locked_server_project",
            "UV_PROJECT_ENVIRONMENT",
        )
    )
    project_sha256 = distribution_runtime.get("runtime_closure_sha256")
    runtime_file_count = distribution_runtime.get("runtime_file_count")
    runtime_bytes = distribution_runtime.get("runtime_bytes")
    jarvis_cd_lock_binding = (
        _jarvis_cd_lock_binding(lock)
        if server_name == "jarvis" and verify_relay_jarvis_cd_lock
        else None
    )
    locked = (
        contract_source_verified
        and uv_identity is not None
        and isinstance(project_sha256, str)
        and is_sha256_text(project_sha256)
        and isinstance(runtime_file_count, int)
        and not isinstance(runtime_file_count, bool)
        and runtime_file_count > 0
        and isinstance(runtime_bytes, int)
        and not isinstance(runtime_bytes, bool)
        and runtime_bytes > 0
    )
    evidence.update(
        {
            "project_sha256": project_sha256,
            "lock_sha256": lock_identity.get("sha256"),
            "runtime_file_count": runtime_file_count,
            "runtime_bytes": runtime_bytes,
            "contract_source_verified": contract_source_verified,
            "locked_runtime_verified": locked,
            "error": (
                None
                if locked
                else "persistent clio-kit launcher contract, lock, or uv executable is unverified"
            ),
        }
    )
    if jarvis_cd_lock_binding is not None:
        evidence["jarvis_cd_lock_binding"] = jarvis_cd_lock_binding
    return evidence


def _locked_clio_kit_runtime_identity(
    install_artifact: dict[str, Any] | None,
    *,
    server_name: str,
    resolved_executable: Path,
    verify_relay_jarvis_cd_lock: bool,
) -> dict[str, Any]:
    """Verify the locked embedded project selected by a clio-kit wheel."""
    wheel_path = (
        Path(str(install_artifact["path"]))
        if install_artifact is not None and isinstance(install_artifact.get("path"), str)
        else None
    )
    uv_identity = _facade()._file_identity(Path(_facade()._resolve_executable("uv")).expanduser())
    evidence: dict[str, Any] = {
        "schema_version": _CLIO_KIT_LOCKED_SERVER_SCHEMA,
        "server_name": server_name,
        "runtime_policy": _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY,
        "project_sha256": None,
        "lock_sha256": None,
        "runtime_file_count": 0,
        "runtime_bytes": 0,
        "contract_source_verified": False,
        "uv_executable": uv_identity,
        "locked_runtime_verified": False,
        "error": None,
    }
    if wheel_path is None or wheel_path.suffix.lower() != ".whl":
        evidence["error"] = "nested clio-kit runtime requires an exact wheel file"
        return evidence
    try:
        with verified_wheel_archive(wheel_path, install_artifact) as wheel:
            members = validated_wheel_members(wheel)
            launcher = members.get("clio_kit/__init__.py")
            if launcher is None or not _zip_member_is_regular(launcher):
                raise ValueError("clio-kit wheel has no unique launcher source")
            launcher_source = read_bounded_zip_member(
                wheel,
                launcher.filename,
                max_bytes=CLIO_KIT_WHEEL_MAX_LAUNCHER_BYTES,
            ).decode("utf-8", errors="strict")
            contract_source_verified = all(
                marker in launcher_source
                for marker in (
                    f'LOCKED_SERVER_LAUNCH_SCHEMA = "{_CLIO_KIT_LOCKED_SERVER_SCHEMA}"',
                    (f'_LOCKED_SERVER_RUNTIME_POLICY = "{_CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY}"'),
                    '"--no-dev"',
                    '"--no-editable"',
                    '"--frozen"',
                    "locked_server_project_identity",
                    "materialize_locked_server_project",
                    "UV_PROJECT_ENVIRONMENT",
                )
            )
            suffix = f"/clio-kit-mcp-servers/{server_name}/uv.lock"
            lock_names = [
                name
                for name in members
                if name.endswith(suffix) or name == f"clio-kit-mcp-servers/{server_name}/uv.lock"
            ]
            if len(lock_names) != 1:
                raise ValueError("clio-kit wheel has no unique embedded server lock")
            lock_name = lock_names[0]
            prefix = lock_name[: -len("uv.lock")]
            inputs = clio_kit_runtime_project_members(
                members,
                prefix=prefix,
                server_name=server_name,
            )
            digest = hashlib.sha256()
            policy = _CLIO_KIT_LOCKED_SERVER_RUNTIME_POLICY.encode("utf-8")
            digest.update(len(policy).to_bytes(8, "big"))
            digest.update(policy)
            digest.update(len(inputs).to_bytes(8, "big"))
            project_bytes = 0
            lock_sha256: str | None = None
            lock_content: bytes | None = None
            for relative, member in inputs:
                encoded = relative.encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
                content_digest = hashlib.sha256()
                content_length = 0
                for chunk in bounded_zip_member_chunks(
                    wheel,
                    member.filename,
                    max_bytes=CLIO_KIT_WHEEL_MAX_PROJECT_BYTES,
                ):
                    project_bytes += len(chunk)
                    if project_bytes > CLIO_KIT_WHEEL_MAX_PROJECT_BYTES:
                        raise ValueError("clio-kit embedded project exceeded its byte limit")
                    content_length += len(chunk)
                    content_digest.update(chunk)
                digest.update(content_length.to_bytes(8, "big"))
                digest.update(content_digest.digest())
                if relative == "uv.lock":
                    lock_sha256 = content_digest.hexdigest()
                    lock_content = read_bounded_zip_member(
                        wheel,
                        member.filename,
                        max_bytes=CLIO_KIT_LOCK_MAX_BYTES,
                    )
            if lock_sha256 is None:
                raise ValueError("clio-kit embedded server project has no lock digest")
            if lock_content is None:
                raise ValueError("clio-kit embedded server project has no readable lock")
    except (
        NotImplementedError,
        OSError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
        zipfile.BadZipFile,
    ) as exc:
        evidence["error"] = f"could not verify locked clio-kit runtime: {exc}"
        return evidence
    jarvis_cd_lock_binding = (
        _jarvis_cd_lock_binding(lock_content)
        if server_name == "jarvis" and verify_relay_jarvis_cd_lock
        else None
    )
    locked = contract_source_verified and uv_identity is not None
    evidence.update(
        {
            "project_sha256": digest.hexdigest(),
            "lock_sha256": lock_sha256,
            "runtime_file_count": len(inputs),
            "runtime_bytes": project_bytes,
            "contract_source_verified": contract_source_verified,
            "locked_runtime_verified": locked,
            "error": (
                None
                if locked
                else "clio-kit locked launcher contract or uv executable is unverified"
            ),
        }
    )
    if jarvis_cd_lock_binding is not None:
        evidence["jarvis_cd_lock_binding"] = jarvis_cd_lock_binding
    return evidence
