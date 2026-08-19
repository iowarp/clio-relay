"""Bootstrap receipt shape/provenance validation (operator-side, post-SSH).

Owner module for validating the v2 bootstrap receipt a remote invocation
returns, plus the JARVIS repository-provenance checks that receipt
validation depends on. Runs entirely on the operator side after the SSH
session returns -- never inside a remotely-executed heredoc, so it carries
no candidate-package constraint.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import cast

from clio_relay.bootstrap_reconcile import (
    LEGACY_MANAGED_JARVIS_REPO_PATH,
    MANAGED_JARVIS_REPO_PATH,
    validate_jarvis_builtin_result,
)
from clio_relay.errors import RelayError


def validate_bootstrap_receipt(
    receipt: dict[str, object],
    *,
    bootstrap_profile: str,
    relay_install_spec: str,
    desired_fingerprint: str,
    expected_jarvis_resource_graph_profile: str | None,
    expected_allow_jarvis_resource_graph_build: bool,
    expected_worker_service: str | None,
) -> None:
    """Validate action-specific v2 evidence from the current remote invocation."""
    install_receipt_sha256 = receipt.get("install_receipt_sha256")
    outcome = receipt.get("outcome")
    duration = receipt.get("duration_seconds")
    components = receipt.get("components")
    operations = receipt.get("operations")
    preservation = receipt.get("preservation")
    worker = receipt.get("worker")
    generation = receipt.get("generation")
    queue_operation = receipt.get("queue_operation")
    jarvis_initialization = receipt.get("jarvis_initialization")
    jarvis_resource_graph = receipt.get("jarvis_resource_graph")
    jarvis_commands = receipt.get("jarvis_commands")
    jarvis_preservation = receipt.get("jarvis_preservation")
    service = receipt.get("service")
    contract = {
        "schema_version": receipt.get("schema_version") == "clio-relay.bootstrap-receipt.v2",
        "bootstrap_profile": receipt.get("bootstrap_profile") == bootstrap_profile,
        "relay_install_spec": receipt.get("relay_install_spec") == relay_install_spec,
        "desired_fingerprint": receipt.get("desired_fingerprint") == desired_fingerprint,
        "outcome": outcome
        in {
            "noop_verified",
            "verified_after_transfer",
            "repaired",
            "reconciled",
            "full",
        },
        "install_receipt_sha256": is_sha256_value(install_receipt_sha256),
        "duration_seconds": (
            not isinstance(duration, bool) and isinstance(duration, (int, float)) and duration >= 0
        ),
        "completed_at": isinstance(receipt.get("completed_at"), str)
        and bool(receipt.get("completed_at")),
        "components": isinstance(components, dict)
        and len(cast(dict[object, object], components)) > 0,
        "operations": isinstance(operations, dict),
        "preservation": isinstance(preservation, dict),
        "worker": isinstance(worker, dict),
        "generation": isinstance(generation, dict),
        "queue_operation": isinstance(queue_operation, dict),
        "jarvis_initialization": isinstance(jarvis_initialization, dict),
        "jarvis_resource_graph": isinstance(jarvis_resource_graph, dict),
        "jarvis_commands": isinstance(jarvis_commands, dict),
        "jarvis_preservation": isinstance(jarvis_preservation, dict),
        "service": isinstance(service, dict),
    }
    failed = sorted(name for name, passed in contract.items() if not passed)
    if failed:
        raise RelayError(f"bootstrap receipt contract failed: {failed}")
    assert isinstance(components, dict)
    typed_components = cast(dict[str, object], components)
    required_components = {"clio-relay", "clio-kit", "jarvis-cd", "jarvis-util", "frp", "uv"}
    if not required_components.issubset(typed_components):
        raise RelayError("bootstrap receipt omitted required component evidence")
    component_actions: dict[str, str] = {}
    for name, raw_evidence in typed_components.items():
        if not isinstance(raw_evidence, dict):
            raise RelayError(f"bootstrap component evidence is invalid: {name}")
        evidence = cast(dict[str, object], raw_evidence)
        action = evidence.get("action")
        component_duration = evidence.get("duration_seconds")
        if (
            action not in {"reused", "prepared", "materialized", "replaced"}
            or not isinstance(evidence.get("observed_identity"), dict)
            or isinstance(component_duration, bool)
            or not isinstance(component_duration, (int, float))
            or component_duration < 0
        ):
            raise RelayError(f"bootstrap component action evidence is invalid: {name}")
        component_actions[name] = cast(str, action)
    assert isinstance(operations, dict)
    typed_operations = cast(dict[str, object], operations)
    count_fields = (
        "download_count",
        "service_restart_count",
        "service_start_count",
        "service_stop_count",
        "service_enable_count",
        "scheduler_submission_count",
        "scheduler_cancellation_count",
        "generation_gc_count",
        "payload_transfer_count",
        "payload_transfer_bytes",
    )
    for field in count_fields:
        value = typed_operations.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RelayError(f"bootstrap operation count is invalid: {field}")
    downloads = typed_operations.get("downloads")
    if not isinstance(downloads, list) or len(cast(list[object], downloads)) != cast(
        int, typed_operations["download_count"]
    ):
        raise RelayError("bootstrap download evidence does not match its count")
    if any(
        typed_operations[field] != 0
        for field in (
            "scheduler_submission_count",
            "scheduler_cancellation_count",
            "generation_gc_count",
        )
    ):
        raise RelayError("bootstrap performed a forbidden scheduler or generation operation")
    payload_count = cast(int, typed_operations["payload_transfer_count"])
    payload_bytes = cast(int, typed_operations["payload_transfer_bytes"])
    if payload_count not in {0, 2} or (payload_count == 0) != (payload_bytes == 0):
        raise RelayError("bootstrap payload transfer evidence is inconsistent")
    assert isinstance(preservation, dict)
    typed_preservation = cast(dict[str, object], preservation)
    if typed_preservation != {
        "scheduler_jobs_cancelled": False,
        "old_generations_retained": True,
        "jarvis_init_on_existing_root": False,
    }:
        raise RelayError("bootstrap preservation evidence is invalid")
    assert isinstance(worker, dict)
    typed_worker = cast(dict[str, object], worker)
    if typed_worker.get("service_name") != expected_worker_service:
        raise RelayError("bootstrap worker service evidence does not match")
    assert isinstance(service, dict)
    typed_service = cast(dict[str, object], service)
    service_pending_install = typed_service.get("pending_install")
    if not isinstance(service_pending_install, bool):
        raise RelayError("bootstrap service pending-install evidence is invalid")
    if expected_worker_service is not None:
        if outcome == "full" and service_pending_install:
            if (
                typed_worker.get("service_was_active") is not False
                or typed_worker.get("service_was_enabled") is not False
                or typed_worker.get("worker_ready") is not False
            ):
                raise RelayError("fresh bootstrap service-pending evidence is inconsistent")
        elif (
            typed_worker.get("service_was_active") is not True
            or typed_worker.get("worker_ready") is not True
            or service_pending_install
        ):
            raise RelayError("managed bootstrap did not leave a ready endpoint service")
    assert isinstance(generation, dict)
    typed_generation = cast(dict[str, object], generation)
    if (
        typed_generation.get("active") != desired_fingerprint
        or not isinstance(typed_generation.get("current_target"), str)
        or not cast(str, typed_generation["current_target"])
    ):
        raise RelayError("bootstrap generation evidence does not prove desired activation")
    assert isinstance(queue_operation, dict)
    typed_queue_operation = cast(dict[str, object], queue_operation)
    queue_action = typed_queue_operation.get("action")
    queue_duration = typed_queue_operation.get("duration_seconds")
    if (
        queue_action not in {"verified_read_only", "audited_and_sealed"}
        or isinstance(queue_duration, bool)
        or not isinstance(queue_duration, (int, float))
        or queue_duration < 0
        or (queue_action == "audited_and_sealed" and queue_duration <= 0)
    ):
        raise RelayError("bootstrap queue action evidence is invalid")
    assert isinstance(jarvis_initialization, dict)
    typed_jarvis_initialization = cast(dict[str, object], jarvis_initialization)
    jarvis_init_action = typed_jarvis_initialization.get("action")
    jarvis_init_duration = typed_jarvis_initialization.get("duration_seconds")
    if (
        jarvis_init_action not in {"preserved", "initialized"}
        or isinstance(jarvis_init_duration, bool)
        or not isinstance(jarvis_init_duration, (int, float))
        or jarvis_init_duration < 0
        or (jarvis_init_action == "initialized" and jarvis_init_duration <= 0)
        or (jarvis_init_action == "preserved" and jarvis_init_duration != 0)
    ):
        raise RelayError("bootstrap JARVIS initialization evidence is invalid")
    assert isinstance(jarvis_resource_graph, dict)
    typed_jarvis_graph = cast(dict[str, object], jarvis_resource_graph)
    jarvis_graph_action = typed_jarvis_graph.get("action")
    jarvis_graph_duration = typed_jarvis_graph.get("duration_seconds")
    jarvis_builtin_result = typed_jarvis_graph.get("builtin_result")
    if (
        set(typed_jarvis_graph)
        != {
            "action",
            "duration_seconds",
            "benchmark_enabled",
            "selected_profile",
            "allow_build_fallback",
            "builtin_result",
        }
        or jarvis_graph_action not in {"preserved", "loaded", "built"}
        or typed_jarvis_graph.get("benchmark_enabled") is not False
        or typed_jarvis_graph.get("selected_profile") != expected_jarvis_resource_graph_profile
        or typed_jarvis_graph.get("allow_build_fallback")
        is not expected_allow_jarvis_resource_graph_build
        or isinstance(jarvis_graph_duration, bool)
        or not isinstance(jarvis_graph_duration, (int, float))
        or jarvis_graph_duration < 0
        or (jarvis_graph_action in {"loaded", "built"} and jarvis_graph_duration <= 0)
        or (jarvis_graph_action == "preserved" and jarvis_graph_duration != 0)
    ):
        raise RelayError("bootstrap JARVIS resource graph evidence is invalid")
    if jarvis_graph_action == "preserved":
        if jarvis_builtin_result is not None:
            raise RelayError("preserved JARVIS graph claimed builtin activation evidence")
    else:
        if expected_jarvis_resource_graph_profile is None or not isinstance(
            jarvis_builtin_result, dict
        ):
            raise RelayError("JARVIS graph activation omitted builtin result evidence")
        try:
            validate_jarvis_builtin_result(
                cast(dict[str, object], jarvis_builtin_result),
                requested_profile=expected_jarvis_resource_graph_profile,
            )
        except ValueError as exc:
            raise RelayError(f"bootstrap JARVIS builtin graph evidence is invalid: {exc}") from exc
        expected_builtin_action = "loaded" if jarvis_graph_action == "loaded" else "unavailable"
        if cast(dict[str, object], jarvis_builtin_result).get("action") != expected_builtin_action:
            raise RelayError("bootstrap JARVIS graph action contradicts builtin evidence")
        if jarvis_graph_action == "built" and not expected_allow_jarvis_resource_graph_build:
            raise RelayError("bootstrap reported an unauthorized JARVIS graph build")
    assert isinstance(jarvis_commands, dict)
    typed_jarvis_commands = cast(dict[str, object], jarvis_commands)
    command_count = typed_jarvis_commands.get("count")
    command_argv = typed_jarvis_commands.get("argv")
    typed_command_argv = cast(list[object], command_argv) if isinstance(command_argv, list) else []
    if (
        isinstance(command_count, bool)
        or not isinstance(command_count, int)
        or command_count < 0
        or not isinstance(command_argv, list)
        or len(typed_command_argv) != command_count
        or any(
            not isinstance(raw_command, list)
            or not raw_command
            or any(
                not isinstance(value, str) or not value for value in cast(list[object], raw_command)
            )
            for raw_command in typed_command_argv
        )
    ):
        raise RelayError("bootstrap JARVIS command evidence is invalid")
    assert isinstance(jarvis_preservation, dict)
    typed_jarvis_preservation = cast(dict[str, object], jarvis_preservation)
    if not isinstance(typed_jarvis_preservation.get("before"), dict) or not isinstance(
        typed_jarvis_preservation.get("after"), dict
    ):
        raise RelayError("bootstrap JARVIS preservation evidence is invalid")
    raw_binding = typed_jarvis_preservation.get("repositories")
    if not isinstance(raw_binding, dict):
        raise RelayError("bootstrap JARVIS repository binding evidence is invalid")
    binding = cast(dict[str, object], raw_binding)
    if set(binding) != {"link_action", "link", "target", "repositories"} or binding.get(
        "link_action"
    ) not in {"reused", "created", "retargeted"}:
        raise RelayError("bootstrap JARVIS repository link evidence is invalid")
    raw_repository_update = binding.get("repositories")
    if not isinstance(raw_repository_update, dict):
        raise RelayError("bootstrap JARVIS repository update evidence is invalid")
    repository_update = cast(dict[str, object], raw_repository_update)
    if set(repository_update) != {
        "action",
        "managed_repo",
        "added_managed_repos",
        "removed_previous_managed_repos",
        "before_sha256",
        "after_sha256",
    } or repository_update.get("action") not in {"reused", "updated"}:
        raise RelayError("bootstrap JARVIS repository update evidence is invalid")
    before_state = cast(dict[str, object], typed_jarvis_preservation["before"])
    after_state = cast(dict[str, object], typed_jarvis_preservation["after"])
    if (
        repository_update.get("before_sha256") != before_state.get("repos_sha256")
        or repository_update.get("after_sha256") != after_state.get("repos_sha256")
        or after_state.get("managed_repo_registered") is not True
        or after_state.get("managed_builtin_repo_registered") is not True
        or not isinstance(repository_update.get("added_managed_repos"), list)
        or not isinstance(repository_update.get("removed_previous_managed_repos"), list)
    ):
        raise RelayError("bootstrap JARVIS repository hashes do not bind preservation evidence")
    if jarvis_graph_action == "loaded" and not is_sha256_value(
        after_state.get("resource_graph_sha256")
    ):
        # The activated graph is JARVIS's normalized derivative of the packaged
        # builtin, so its digest legitimately differs from source_sha256 and
        # cannot be bound by equality here (#158). Source identity is proven
        # remotely, where the packaged file actually lives -- the cluster-side
        # step hashes the source JARVIS names and fails closed on a mismatch.
        # What the host binds is that activation evidence was recorded at all.
        raise RelayError("loaded JARVIS graph did not record its activated resource graph digest")
    if outcome == "noop_verified":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
            or typed_operations["service_restart_count"] != 0
            or typed_operations["service_start_count"] != 0
            or typed_operations["service_stop_count"] != 0
            or typed_operations["service_enable_count"] != 0
            or typed_operations["payload_transfer_count"] != 0
            or typed_operations["payload_transfer_bytes"] != 0
            or queue_action != "verified_read_only"
            or queue_duration != 0
            or jarvis_init_action != "preserved"
            or jarvis_graph_action != "preserved"
            or command_count != 0
            or typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or typed_jarvis_preservation.get("repositories_byte_identical") is not True
            or binding.get("link_action") != "reused"
            or repository_update.get("action") != "reused"
        ):
            raise RelayError("bootstrap no-op receipt reported mutation")
    elif outcome == "verified_after_transfer":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
            or typed_operations["service_restart_count"] != 0
            or typed_operations["service_start_count"] != 0
            or typed_operations["service_stop_count"] != 0
            or typed_operations["service_enable_count"] != 0
            or payload_count != 2
            or payload_bytes <= 0
            or queue_action != "verified_read_only"
            or queue_duration != 0
            or jarvis_init_action != "preserved"
            or jarvis_graph_action != "preserved"
            or command_count != 0
            or typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or typed_jarvis_preservation.get("repositories_byte_identical") is not True
            or binding.get("link_action") != "reused"
            or repository_update.get("action") != "reused"
        ):
            raise RelayError("post-transfer verification receipt reported mutation")
    elif outcome == "repaired":
        if (
            any(action != "reused" for action in component_actions.values())
            or typed_operations["download_count"] != 0
        ):
            raise RelayError("bootstrap repair receipt reported component replacement")
        if jarvis_init_action != "preserved":
            raise RelayError("bootstrap repair receipt reported JARVIS initialization")
        if jarvis_graph_action != "preserved" or command_count != 0:
            raise RelayError("bootstrap repair receipt reported JARVIS commands")
        managed_repo = repository_update.get("managed_repo")
        added_repositories = repository_update.get("added_managed_repos")
        removed_repositories = repository_update.get("removed_previous_managed_repos")
        if (
            typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
        ):
            raise RelayError("bootstrap repair receipt reported JARVIS state mutation")
        if (
            not isinstance(managed_repo, str)
            or not PurePosixPath(managed_repo).is_absolute()
            or any(character in managed_repo for character in "\x00\r\n")
            or binding.get("link") != managed_repo
            or binding.get("link_action") not in {"reused", "created", "retargeted"}
            or not isinstance(added_repositories, list)
            or not isinstance(removed_repositories, list)
            or not is_sha256_value(typed_generation.get("previous"))
        ):
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        managed_suffix = MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        if not managed_repo.endswith(managed_suffix):
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        remote_home = managed_repo[: -len(managed_suffix)]
        expected_target = (
            remote_home + "/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        )
        expected_previous = remote_home + "/.local/src/clio-relay/jarvis-packages/clio_relay"
        expected_legacy = remote_home + LEGACY_MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        expected_builtin = remote_home + "/.ppi-jarvis/builtin"
        typed_removed_repositories = cast(list[object], removed_repositories)
        removed_repositories_are_proven = _jarvis_repository_removals_are_proven(
            typed_removed_repositories,
            remote_home=remote_home,
            exact_paths={expected_previous, expected_legacy},
        )
        repository_action = repository_update.get("action")
        if binding.get("target") != expected_target:
            raise RelayError("bootstrap managed JARVIS binding repair is invalid")
        if repository_action == "reused":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not True
                or added_repositories
                or removed_repositories
            ):
                raise RelayError("bootstrap managed JARVIS repository reuse is invalid")
        elif repository_action == "updated":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not False
                or not _jarvis_repository_additions_are_proven(
                    cast(list[object], added_repositories),
                    managed_repo=managed_repo,
                    managed_builtin_repo=expected_builtin,
                )
                or not removed_repositories_are_proven
                or (not added_repositories and not removed_repositories)
            ):
                raise RelayError("bootstrap managed JARVIS repository repair is invalid")
        else:
            raise RelayError("bootstrap managed JARVIS repository repair is invalid")
    elif outcome == "reconciled":
        raw_transaction = receipt.get("transaction")
        transaction_mode = (
            cast(dict[str, object], raw_transaction).get("mode")
            if isinstance(raw_transaction, dict)
            else None
        )
        if transaction_mode == "component-upgrade":
            expected_actions = {
                "clio-relay": "replaced",
                "clio-kit": "replaced",
                "jarvis-cd": "replaced",
                "jarvis-util": "reused",
                "frp": "reused",
                "uv": "reused",
            }
        elif transaction_mode == "relay-only":
            expected_actions = {
                "clio-relay": "prepared",
                "clio-kit": "reused",
                "jarvis-cd": "reused",
                "jarvis-util": "reused",
                "frp": "reused",
                "uv": "reused",
            }
        else:
            raise RelayError("reconciled bootstrap receipt has an invalid transaction mode")
        if any(component_actions.get(name) != action for name, action in expected_actions.items()):
            raise RelayError("staged reconcile receipt has invalid component actions")
        if jarvis_init_action != "preserved":
            raise RelayError("staged reconcile reported JARVIS initialization")
        if jarvis_graph_action != "preserved" or command_count != 0:
            raise RelayError("staged reconcile reported JARVIS commands")
        previous_generation = typed_generation.get("previous")
        link_action = binding.get("link_action")
        previous_generation_is_proven = bool(
            previous_generation == "legacy" or is_sha256_value(previous_generation)
        )
        link_action_is_proven = bool(
            link_action == "reused"
            or (link_action in {"created", "retargeted"} and previous_generation_is_proven)
        )
        if (
            typed_jarvis_preservation.get("config_byte_identical") is not True
            or typed_jarvis_preservation.get("resource_graph_byte_identical") is not True
            or not link_action_is_proven
        ):
            raise RelayError("staged reconcile did not preserve existing JARVIS state")
        managed_repo = repository_update.get("managed_repo")
        added_repositories = repository_update.get("added_managed_repos")
        removed_repositories = repository_update.get("removed_previous_managed_repos")
        if (
            not isinstance(managed_repo, str)
            or not PurePosixPath(managed_repo).is_absolute()
            or any(character in managed_repo for character in "\x00\r\n")
            or binding.get("link") != managed_repo
        ):
            raise RelayError("staged reconcile repository binding is invalid")
        managed_suffix = MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        if not managed_repo.endswith(managed_suffix):
            raise RelayError("staged reconcile repository binding is invalid")
        remote_home = managed_repo[: -len(managed_suffix)]
        expected_target = (
            remote_home + "/.local/share/clio-relay/current/source/jarvis-packages/clio_relay"
        )
        expected_previous = remote_home + "/.local/src/clio-relay/jarvis-packages/clio_relay"
        expected_legacy = remote_home + LEGACY_MANAGED_JARVIS_REPO_PATH.removeprefix("~")
        expected_builtin = remote_home + "/.ppi-jarvis/builtin"
        typed_removed_repositories = cast(list[object], removed_repositories)
        removed_repositories_are_proven = _jarvis_repository_removals_are_proven(
            typed_removed_repositories,
            remote_home=remote_home,
            exact_paths={expected_previous, expected_legacy},
        )
        repository_action = repository_update.get("action")
        if (
            binding.get("target") != expected_target
            or not isinstance(added_repositories, list)
            or not isinstance(removed_repositories, list)
        ):
            raise RelayError("staged reconcile repository binding is invalid")
        if repository_action == "reused":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not True
                or added_repositories
                or removed_repositories
            ):
                raise RelayError("staged reconcile repository reuse is invalid")
        elif repository_action == "updated":
            if (
                typed_jarvis_preservation.get("repositories_byte_identical") is not False
                or not _jarvis_repository_additions_are_proven(
                    cast(list[object], added_repositories),
                    managed_repo=managed_repo,
                    managed_builtin_repo=expected_builtin,
                )
                or not removed_repositories_are_proven
                or (not added_repositories and not removed_repositories)
            ):
                raise RelayError("staged reconcile repository migration is invalid")
        else:  # pragma: no cover - rejected by the generic evidence contract above
            raise RelayError("staged reconcile repository action is invalid")
        if payload_count != 2 or payload_bytes <= 0:
            raise RelayError("staged reconcile omitted its transferred payload evidence")
    elif outcome == "full":
        if any(action != "prepared" for action in component_actions.values()):
            raise RelayError("fresh bootstrap receipt has invalid component actions")
        if jarvis_init_action != "initialized":
            raise RelayError("fresh bootstrap did not report JARVIS initialization")
        expected_command_count = 2 if jarvis_graph_action == "loaded" else 3
        if (
            jarvis_graph_action not in {"loaded", "built"}
            or command_count != expected_command_count
        ):
            raise RelayError("fresh bootstrap did not report exact graph activation commands")
        expected_graph_commands: list[list[str]] = [
            [
                "jarvis",
                "rg",
                "load-builtin",
                cast(str, expected_jarvis_resource_graph_profile),
                "+json",
            ]
        ]
        if jarvis_graph_action == "built":
            expected_graph_commands.append(["jarvis", "rg", "build", "+no_benchmark"])
        if cast(list[object], command_argv)[1:] != expected_graph_commands:
            raise RelayError("fresh bootstrap reported unexpected graph commands")
        if payload_count != 2 or payload_bytes <= 0:
            raise RelayError("fresh bootstrap omitted its transferred payload evidence")


def is_sha256_value(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _jarvis_repository_removals_are_proven(
    values: list[object],
    *,
    remote_home: str,
    exact_paths: set[str],
) -> bool:
    """Accept only exact historical relay repository paths in bootstrap evidence."""
    if not all(isinstance(value, str) for value in values):
        return False
    repositories = cast(list[str], values)
    return bool(
        repositories == sorted(set(repositories))
        and all(
            value in exact_paths
            or _is_relay_owned_jarvis_builtin_repository(value, remote_home=remote_home)
            for value in repositories
        )
    )


def _jarvis_repository_additions_are_proven(
    values: list[object],
    *,
    managed_repo: str,
    managed_builtin_repo: str,
) -> bool:
    """Accept only the ordered relay and JARVIS stable repository bindings."""
    allowed = [managed_repo, managed_builtin_repo]
    if not all(isinstance(value, str) for value in values):
        return False
    repositories = cast(list[str], values)
    return bool(
        len(repositories) <= len(allowed)
        and len(repositories) == len(set(repositories))
        and all(value in allowed for value in repositories)
        and repositories == [value for value in allowed if value in repositories]
    )


def _is_relay_owned_jarvis_builtin_repository(value: str, *, remote_home: str) -> bool:
    """Recognize the exact legacy or content-addressed relay venv builtin path."""
    if any(character in value for character in "\x00\r\n"):
        return False
    path = PurePosixPath(value)
    home = PurePosixPath(remote_home)
    if not path.is_absolute() or str(path) != value or not home.is_absolute():
        return False
    try:
        relative = path.relative_to(home / ".local/share/clio-relay")
    except ValueError:
        return False
    parts = relative.parts
    legacy_prefix = ("jarvis-venv",)
    generation_prefix = (
        ("generations", parts[1], "jarvis-venv")
        if len(parts) >= 3 and is_sha256_value(parts[1])
        else ()
    )
    for prefix in (legacy_prefix, generation_prefix):
        if not prefix or parts[: len(prefix)] != prefix:
            continue
        suffix = parts[len(prefix) :]
        if (
            len(suffix) == 4
            and suffix[0] in {"lib", "lib64"}
            and _is_python_library_directory_name(suffix[1])
            and suffix[2:] == ("site-packages", "builtin")
        ):
            return True
    return False


def _is_python_library_directory_name(value: str) -> bool:
    """Recognize one CPython virtual-environment library component."""
    if not value.startswith("python"):
        return False
    major, separator, minor = value.removeprefix("python").partition(".")
    return bool(separator and major.isdigit() and minor.isdigit())
