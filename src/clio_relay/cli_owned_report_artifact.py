"""``_persist_local_cleanup_report_artifact`` (iowarp/clio-relay#231
continuation): the local, chunked, checksummed retention of one exact
coordinator cleanup report, called once by ``session teardown``. Moved
as its own file because at 810 lines it is too large to share a home
with anything else and still land near the 150-500 line target; see
``scripts/check_file_size.py``'s ``RATCHET_BASELINE`` entry for why it
still exceeds the 800-line new-file cap by a small margin (splitting
its own internal sequential writes into separate functions was judged
higher-risk than a documented, verbatim, ratcheted-in placement)."""

from __future__ import annotations

import ctypes
import hashlib
import hmac
import json
import os
import re
import stat
from json import JSONDecodeError
from pathlib import Path
from typing import cast

import clio_relay.cli_cleanup_evidence as cli_cleanup_evidence
import clio_relay.cluster_config as cluster_config
from clio_relay.cluster_config import (
    ensure_private_configuration_windows_handle,
    open_private_configuration_windows_descriptor,
    release_private_configuration_windows_parent_guard,
)
from clio_relay.errors import RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.session_lifecycle import (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    SessionLifecycleReport,
    session_lifecycle_report_bytes,
)
from clio_relay.validation_report import (
    durably_ensure_validation_directory,
)

MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES = 8 * 1024 * 1024


MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES = 64 * 1024


MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES = 11


MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES = 2 * (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES
)


_LOCAL_CLEANUP_REPORT_ARTIFACT_DIRECTORY_NAME = "cleanup-evidence-v1"


_LOCAL_CLEANUP_REPORT_ARTIFACT_PATTERN = re.compile(r"^r-[0-9a-f]{64}\.(?:p[0-9]{4}|manifest)$")


_LOCAL_CLEANUP_REPORT_PENDING_PATTERN = re.compile(
    r"^\.r-[0-9a-f]{64}\.(?:p[0-9]{4}|manifest)\.pending$"
)


def _persist_local_cleanup_report_artifact(
    report: SessionLifecycleReport,
    *,
    validation_report_path: Path,
    evidence_lock: cli_cleanup_evidence._CleanupEvidenceLock | None = None,
) -> cli_cleanup_evidence._LocalCleanupReportArtifact:
    """Persist one exact report in a private, report-owned bounded artifact directory."""
    payload = session_lifecycle_report_bytes(report)
    digest = hashlib.sha256(payload).hexdigest()
    chunk_specs: list[tuple[str, bytes, str]] = []
    for index, offset in enumerate(range(0, len(payload), MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES)):
        chunk = payload[offset : offset + MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES]
        chunk_specs.append(
            (
                f"r-{digest}.p{index:04d}",
                chunk,
                hashlib.sha256(chunk).hexdigest(),
            )
        )
    manifest_payload = json.dumps(
        {
            "schema_version": "clio-relay.local-cleanup-report-artifact.v1",
            "encoding": "canonical-json-chunks",
            "report_sha256": digest,
            "report_size": len(payload),
            "chunk_size_limit": MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES,
            "chunks": [
                {"name": name, "size": len(chunk), "sha256": sha256}
                for name, chunk, sha256 in chunk_specs
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(manifest_payload) > MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES:
        raise RelayError("local cleanup report artifact manifest exceeds its byte limit")
    manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
    manifest_name = f"r-{digest}.manifest"
    expected_names = {manifest_name, *(name for name, _chunk, _sha256 in chunk_specs)}
    expected_names.update(f".{name}.pending" for name in tuple(expected_names))

    if not validation_report_path.name or validation_report_path.name in {".", ".."}:
        raise RelayError("validation report path has no safe artifact identity")
    requested_parent = cli_cleanup_evidence._cleanup_evidence_state_parent()
    if evidence_lock is not None:
        cli_cleanup_evidence._verify_cleanup_evidence_lock(
            evidence_lock,
            expected_parent=requested_parent,
        )
    locked_posix_parent_fd = (
        evidence_lock.parent_fd if evidence_lock is not None and os.name == "posix" else None
    )
    if os.name == "posix" and evidence_lock is not None and locked_posix_parent_fd is None:
        raise RelayError("cleanup evidence lock omitted its pinned POSIX parent")
    if locked_posix_parent_fd is not None:
        if evidence_lock is None:  # pragma: no cover - narrowed by descriptor selection
            raise RelayError("cleanup evidence lock disappeared while binding its parent")
        parent_directory = evidence_lock.path.parent
        parent_linked = os.fstat(locked_posix_parent_fd)
    else:
        durably_ensure_validation_directory(requested_parent)
        requested_parent_status = os.lstat(requested_parent)
        if stat.S_ISLNK(requested_parent_status.st_mode):
            raise RelayError("local cleanup report artifact parent cannot be a symlink")
        if os.name == "nt":
            requested_anchor = cli_cleanup_evidence._open_windows_pinned_directory(
                requested_parent,
                expected=requested_parent_status,
            )
            cli_cleanup_evidence._close_windows_pinned_directory(requested_anchor)
        parent_directory = requested_parent.resolve(strict=True)
        if os.path.normcase(str(parent_directory)) != os.path.normcase(str(requested_parent)):
            raise RelayError("local cleanup report artifact parent cannot traverse a reparse point")
        parent_linked = os.lstat(parent_directory)
    if not stat.S_ISDIR(parent_linked.st_mode) or stat.S_ISLNK(parent_linked.st_mode):
        raise RelayError("local cleanup report artifact parent is not a real directory")
    if os.name == "posix" and not (
        (parent_linked.st_uid == os.geteuid() and stat.S_IMODE(parent_linked.st_mode) & 0o022 == 0)
        or (parent_linked.st_uid == 0 and stat.S_IMODE(parent_linked.st_mode) & stat.S_ISVTX != 0)
    ):
        raise RelayError("local cleanup report artifact parent is not rename-safe")
    artifact_directory_name = _LOCAL_CLEANUP_REPORT_ARTIFACT_DIRECTORY_NAME
    artifact_directory = parent_directory / artifact_directory_name
    parent_fd: int | None = None
    directory_fd: int | None = None
    parent_windows_anchor: cli_cleanup_evidence._WindowsPinnedDirectory | None = None
    directory_windows_anchor: cli_cleanup_evidence._WindowsPinnedDirectory | None = None
    directory_windows_guard: tuple[Path, ctypes.c_void_p] | None = None
    if os.name == "posix":
        try:
            parent_fd = (
                os.dup(locked_posix_parent_fd)
                if locked_posix_parent_fd is not None
                else os.open(
                    parent_directory,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                )
            )
            if not os.path.samestat(parent_linked, os.fstat(parent_fd)):
                raise RelayError("local cleanup report artifact parent changed while opening")
            created = False
            try:
                os.mkdir(artifact_directory_name, 0o700, dir_fd=parent_fd)
                created = True
            except FileExistsError:
                pass
            if created:
                os.fsync(parent_fd)
            directory_linked = os.stat(
                artifact_directory_name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            directory_fd = os.open(
                artifact_directory_name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            if directory_fd is not None:
                os.close(directory_fd)
            if parent_fd is not None:
                os.close(parent_fd)
            raise RelayError(
                f"local cleanup report artifact directory cannot be pinned: {exc}"
            ) from exc
        directory_opened = os.fstat(directory_fd)
        if evidence_lock is not None and (
            evidence_lock.parent_fd is None
            or not os.path.samestat(os.fstat(evidence_lock.parent_fd), os.fstat(parent_fd))
        ):
            os.close(directory_fd)
            os.close(parent_fd)
            raise RelayError("local cleanup report artifact parent differs from its evidence lock")
        if not (
            stat.S_ISDIR(directory_linked.st_mode)
            and not stat.S_ISLNK(directory_linked.st_mode)
            and directory_linked.st_uid == os.geteuid()
            and stat.S_IMODE(directory_linked.st_mode) & 0o077 == 0
            and os.path.samestat(directory_linked, directory_opened)
        ):
            os.close(directory_fd)
            os.close(parent_fd)
            raise RelayError("local cleanup report artifact directory changed while opening")
    else:
        try:
            parent_windows_anchor = cli_cleanup_evidence._open_windows_pinned_directory(
                parent_directory,
                expected=parent_linked,
            )
            durably_ensure_validation_directory(artifact_directory)
            directory_linked = os.lstat(artifact_directory)
            if not stat.S_ISDIR(directory_linked.st_mode) or stat.S_ISLNK(directory_linked.st_mode):
                raise RelayError("local cleanup report artifact directory is not a real directory")
            directory_windows_anchor = cli_cleanup_evidence._open_windows_pinned_directory(
                artifact_directory,
                expected=directory_linked,
                acl_write=True,
            )
            ensure_private_configuration_windows_handle(
                internal_filesystem_path(artifact_directory, force_extended=True),
                handle=directory_windows_anchor.handle,
                directory=True,
            )
            directory_windows_guard = (
                cluster_config.acquire_private_configuration_windows_parent_guard(
                    artifact_directory
                )
            )
            cli_cleanup_evidence._verify_windows_pinned_directory(directory_windows_anchor)
            if evidence_lock is not None:
                cli_cleanup_evidence._verify_cleanup_evidence_lock(
                    evidence_lock,
                    expected_parent=parent_directory,
                )
                if evidence_lock.windows_parent is None or not os.path.samestat(
                    evidence_lock.windows_parent.status,
                    parent_windows_anchor.status,
                ):
                    raise RelayError(
                        "local cleanup report artifact parent differs from its evidence lock"
                    )
        except BaseException:
            try:
                cli_cleanup_evidence._close_windows_pinned_directory(directory_windows_anchor)
            finally:
                try:
                    cli_cleanup_evidence._close_windows_pinned_directory(parent_windows_anchor)
                finally:
                    release_private_configuration_windows_parent_guard(directory_windows_guard)
            raise

    parent_fd = cli_cleanup_evidence._optional_runtime_descriptor(parent_fd)
    directory_fd = cli_cleanup_evidence._optional_runtime_descriptor(directory_fd)
    ignored_internal_names = cli_cleanup_evidence._windows_parent_guard_names(
        directory_windows_guard
    )

    def verify_directory() -> None:
        try:
            observed_parent = os.lstat(parent_directory)
            observed = (
                os.stat(
                    artifact_directory_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if parent_fd is not None
                else os.lstat(artifact_directory)
            )
        except OSError as exc:
            raise RelayError("local cleanup report artifact directory disappeared") from exc
        if not (
            os.path.samestat(parent_linked, observed_parent)
            and os.path.samestat(directory_linked, observed)
        ):
            raise RelayError("local cleanup report artifact directory identity changed")
        if os.name == "nt":
            cli_cleanup_evidence._verify_windows_pinned_directory(parent_windows_anchor)
            cli_cleanup_evidence._verify_windows_pinned_directory(directory_windows_anchor)

    def stat_name(name: str) -> os.stat_result | None:
        if Path(name).name != name:
            raise RelayError("local cleanup report artifact name is unsafe")
        try:
            if directory_fd is not None:
                return os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            return os.lstat(artifact_directory / name)
        except FileNotFoundError:
            return None

    def fsync_directory() -> None:
        if directory_fd is not None:
            os.fsync(directory_fd)
        verify_directory()

    def unlink_name(name: str) -> None:
        if directory_fd is not None:
            os.unlink(name, dir_fd=directory_fd)
        else:
            os.unlink(artifact_directory / name)
        fsync_directory()

    def read_exact(
        name: str,
        *,
        expected_size: int,
        required: bool,
        expected_nlink: int = 1,
    ) -> bytes | None:
        descriptor: int | None = None
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            if directory_fd is not None:
                descriptor = os.open(name, flags, dir_fd=directory_fd)
            elif os.name == "nt":
                descriptor = open_private_configuration_windows_descriptor(
                    internal_filesystem_path(
                        artifact_directory / name,
                        force_extended=True,
                    ),
                    expected_nlink=expected_nlink,
                )
            else:
                descriptor = os.open(artifact_directory / name, flags)
        except FileNotFoundError:
            if required:
                raise RelayError("local cleanup report artifact disappeared") from None
            return None
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact cannot be opened safely: {exc}"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            linked = stat_name(name)
            if linked is None:  # pragma: no cover - opened descriptor exists
                raise RelayError("local cleanup report artifact pathname disappeared")
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == expected_nlink
                and linked.st_nlink == expected_nlink
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
                and opened.st_size == expected_size
            ):
                raise RelayError("local cleanup report artifact is not one exact regular file")
            if os.name == "posix" and not (
                opened.st_uid == os.geteuid()
                and linked.st_uid == os.geteuid()
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(linked.st_mode) == 0o600
            ):
                raise RelayError("local cleanup report artifact is not owner-private")
            value = bytearray()
            while len(value) <= expected_size:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, expected_size + 1 - len(value)),
                )
                if not chunk:
                    break
                value.extend(chunk)
            final_opened = os.fstat(descriptor)
            final_linked = stat_name(name)
            initial_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
                opened.st_nlink,
            )
            final_identity = (
                final_opened.st_dev,
                final_opened.st_ino,
                final_opened.st_size,
                final_opened.st_mtime_ns,
                final_opened.st_ctime_ns,
                final_opened.st_nlink,
            )
            if (
                len(value) != expected_size
                or final_linked is None
                or final_identity != initial_identity
                or (final_linked.st_dev, final_linked.st_ino, final_linked.st_nlink)
                != (final_opened.st_dev, final_opened.st_ino, expected_nlink)
            ):
                raise RelayError("local cleanup report artifact changed while it was read")
            verify_directory()
            return bytes(value)
        finally:
            os.close(descriptor)

    def candidate_byte_limit(name: str) -> int:
        return (
            MAX_LOCAL_CLEANUP_REPORT_MANIFEST_BYTES
            if ".manifest" in name
            else MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
        )

    def verify_candidate_status(
        name: str,
        observed: os.stat_result,
        *,
        expected_nlink: int,
    ) -> None:
        if not (
            stat.S_ISREG(observed.st_mode)
            and observed.st_nlink == expected_nlink
            and 0 <= observed.st_size <= candidate_byte_limit(name)
        ):
            raise RelayError("local cleanup report artifact candidate is unsafe")
        if os.name == "posix" and not (
            observed.st_uid == os.geteuid() and stat.S_IMODE(observed.st_mode) == 0o600
        ):
            raise RelayError("local cleanup report artifact candidate is not owner-private")

    def unlink_verified_candidate(
        name: str,
        observed: os.stat_result,
        *,
        expected_nlink: int,
    ) -> None:
        verify_candidate_status(name, observed, expected_nlink=expected_nlink)
        current = stat_name(name)
        if current is None or (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_nlink,
        ) != (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            expected_nlink,
        ):
            raise RelayError("local cleanup report artifact changed before deletion")
        unlink_name(name)

    def scan_candidates() -> dict[str, os.stat_result]:
        candidates: dict[str, os.stat_result] = {}
        stored_bytes = 0
        observed_inodes: set[tuple[int, int]] = set()
        verify_directory()
        try:
            with os.scandir(directory_fd if directory_fd is not None else artifact_directory) as it:
                for entry in it:
                    name = entry.name
                    if name in ignored_internal_names:
                        continue
                    if not (
                        _LOCAL_CLEANUP_REPORT_ARTIFACT_PATTERN.fullmatch(name)
                        or _LOCAL_CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name)
                    ):
                        raise RelayError(
                            "local cleanup report artifact directory contains an invalid entry"
                        )
                    if len(candidates) >= MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES:
                        raise RelayError(
                            "local cleanup report artifact directory exceeds its entry limit"
                        )
                    observed = stat_name(name)
                    if observed is None:
                        raise RelayError(
                            "local cleanup report artifact disappeared during enumeration"
                        )
                    inode_identity = (observed.st_dev, observed.st_ino)
                    if inode_identity not in observed_inodes:
                        stored_bytes += observed.st_size
                        observed_inodes.add(inode_identity)
                    if stored_bytes > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES:
                        raise RelayError(
                            "local cleanup report artifact directory exceeds its byte limit"
                        )
                    candidates[name] = observed
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact directory cannot be enumerated: {exc}"
            ) from exc
        verify_directory()
        return candidates

    def prune_unreferenced_candidates(*, preserve_names: set[str]) -> None:
        candidates = scan_candidates()
        remaining = set(candidates)
        for pending_name in sorted(
            name
            for name in remaining
            if _LOCAL_CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name) and name not in preserve_names
        ):
            if pending_name not in remaining:
                continue
            final_name = pending_name[1 : -len(".pending")]
            pending_status = candidates[pending_name]
            if final_name in remaining and final_name not in preserve_names:
                final_status = candidates[final_name]
                if not (
                    (pending_status.st_dev, pending_status.st_ino)
                    == (final_status.st_dev, final_status.st_ino)
                    and pending_status.st_nlink == 2
                    and final_status.st_nlink == 2
                ):
                    raise RelayError(
                        "local cleanup report artifact pruning found an ambiguous link pair"
                    )
                verify_candidate_status(pending_name, pending_status, expected_nlink=2)
                verify_candidate_status(final_name, final_status, expected_nlink=2)
                unlink_verified_candidate(pending_name, pending_status, expected_nlink=2)
                refreshed_final = stat_name(final_name)
                if refreshed_final is None:
                    raise RelayError("local cleanup report artifact disappeared while pruning")
                unlink_verified_candidate(final_name, refreshed_final, expected_nlink=1)
                remaining.remove(pending_name)
                remaining.remove(final_name)
                continue
            unlink_verified_candidate(pending_name, pending_status, expected_nlink=1)
            remaining.remove(pending_name)
        for name in sorted(remaining - preserve_names):
            unlink_verified_candidate(name, candidates[name], expected_nlink=1)
            remaining.remove(name)
        observed = set(scan_candidates())
        if not observed.issubset(preserve_names):
            raise RelayError("local cleanup report artifact pruning was not exact")

    def complete_report_names(
        candidates: dict[str, os.stat_result],
        *,
        candidate_digest: str,
    ) -> set[str]:
        manifest_candidate_name = f"r-{candidate_digest}.manifest"
        manifest_status = candidates.get(manifest_candidate_name)
        if manifest_status is None:
            raise RelayError("retained cleanup report artifact has no manifest")
        verify_candidate_status(
            manifest_candidate_name,
            manifest_status,
            expected_nlink=1,
        )
        manifest_bytes = read_exact(
            manifest_candidate_name,
            expected_size=manifest_status.st_size,
            required=True,
        )
        try:
            manifest_value = json.loads((manifest_bytes or b"").decode("utf-8"))
        except (UnicodeDecodeError, JSONDecodeError) as exc:
            raise RelayError("retained cleanup report artifact manifest is invalid") from exc
        if not isinstance(manifest_value, dict):
            raise RelayError("retained cleanup report artifact manifest is not an object")
        manifest = cast(dict[str, object], manifest_value)
        raw_report_size = manifest.get("report_size")
        raw_chunks = manifest.get("chunks")
        if not (
            manifest.get("schema_version") == "clio-relay.local-cleanup-report-artifact.v1"
            and manifest.get("encoding") == "canonical-json-chunks"
            and manifest.get("report_sha256") == candidate_digest
            and manifest.get("chunk_size_limit") == MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
            and isinstance(raw_report_size, int)
            and not isinstance(raw_report_size, bool)
            and 0 < raw_report_size <= MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES
            and isinstance(raw_chunks, list)
            and 0
            < len(cast(list[object], raw_chunks))
            <= (MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES + MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES - 1)
            // MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
        ):
            raise RelayError("retained cleanup report artifact manifest is inconsistent")
        retained = {manifest_candidate_name}
        observed_report_size = 0
        report_hasher = hashlib.sha256()
        for index, raw_chunk in enumerate(cast(list[object], raw_chunks)):
            if not isinstance(raw_chunk, dict):
                raise RelayError("retained cleanup report artifact chunk is invalid")
            chunk = cast(dict[str, object], raw_chunk)
            chunk_name = f"r-{candidate_digest}.p{index:04d}"
            chunk_size = chunk.get("size")
            chunk_sha256 = chunk.get("sha256")
            if not (
                chunk.get("name") == chunk_name
                and isinstance(chunk_size, int)
                and not isinstance(chunk_size, bool)
                and 0 < chunk_size <= MAX_LOCAL_CLEANUP_REPORT_CHUNK_BYTES
                and isinstance(chunk_sha256, str)
                and re.fullmatch(r"[0-9a-f]{64}", chunk_sha256)
            ):
                raise RelayError("retained cleanup report artifact chunk metadata is invalid")
            chunk_status = candidates.get(chunk_name)
            if chunk_status is None or chunk_status.st_size != chunk_size:
                raise RelayError("retained cleanup report artifact chunk is missing")
            verify_candidate_status(chunk_name, chunk_status, expected_nlink=1)
            chunk_bytes = read_exact(
                chunk_name,
                expected_size=chunk_size,
                required=True,
            )
            if chunk_bytes is None or hashlib.sha256(chunk_bytes).hexdigest() != chunk_sha256:
                raise RelayError("retained cleanup report artifact chunk digest is invalid")
            observed_report_size += chunk_size
            report_hasher.update(chunk_bytes)
            retained.add(chunk_name)
        if observed_report_size != raw_report_size or report_hasher.hexdigest() != candidate_digest:
            raise RelayError("retained cleanup report artifact size or digest is inconsistent")
        return retained

    def newest_previous_complete_report_names(
        candidates: dict[str, os.stat_result],
    ) -> set[str]:
        manifests: list[tuple[int, str]] = []
        for name, observed in candidates.items():
            match = re.fullmatch(r"r-([0-9a-f]{64})\.manifest", name)
            if match is not None and match.group(1) != digest:
                manifests.append((observed.st_mtime_ns, match.group(1)))
        if not manifests:
            return set()
        _mtime_ns, previous_digest = max(manifests)
        return complete_report_names(
            candidates,
            candidate_digest=previous_digest,
        )

    def publish_exact(name: str, content: bytes, *, expected_sha256: str) -> None:
        pending_name = f".{name}.pending"
        final_status = stat_name(name)
        pending_status = stat_name(pending_name)
        if final_status is not None and pending_status is not None:
            safe_link_window = bool(
                stat.S_ISREG(final_status.st_mode)
                and stat.S_ISREG(pending_status.st_mode)
                and final_status.st_nlink == 2
                and pending_status.st_nlink == 2
                and (final_status.st_dev, final_status.st_ino)
                == (pending_status.st_dev, pending_status.st_ino)
            )
            if os.name == "posix":
                safe_link_window = bool(
                    safe_link_window
                    and final_status.st_uid == os.geteuid()
                    and pending_status.st_uid == os.geteuid()
                    and stat.S_IMODE(final_status.st_mode) == 0o600
                    and stat.S_IMODE(pending_status.st_mode) == 0o600
                )
            if not safe_link_window:
                raise RelayError("local cleanup report artifact publication is ambiguous")
            linked = read_exact(
                pending_name,
                expected_size=len(content),
                required=True,
                expected_nlink=2,
            )
            if linked is None or not hmac.compare_digest(linked, content):
                raise RelayError("local cleanup report artifact linked file differs")
            unlink_name(pending_name)
            final_status = stat_name(name)
            pending_status = None
        existing = read_exact(name, expected_size=len(content), required=False)
        if existing is not None:
            if hashlib.sha256(existing).hexdigest() != expected_sha256:
                raise RelayError("local cleanup report artifact digest did not match its name")
            return
        if final_status is not None:
            raise RelayError("local cleanup report artifact final path was not readable")
        descriptor: int | None = None
        try:
            if pending_status is not None:
                verify_candidate_status(pending_name, pending_status, expected_nlink=1)
                staged = (
                    read_exact(
                        pending_name,
                        expected_size=len(content),
                        required=True,
                    )
                    if pending_status.st_size == len(content)
                    else None
                )
                if staged is None or not hmac.compare_digest(staged, content):
                    # Pending-only content is unreferenced staging.  A proven
                    # owner-private single link may be removed and restaged
                    # after an interrupted write.
                    unlink_verified_candidate(
                        pending_name,
                        pending_status,
                        expected_nlink=1,
                    )
                    pending_status = None
            if pending_status is None:
                if os.name == "nt":
                    pending_path = internal_filesystem_path(
                        artifact_directory / pending_name,
                        force_extended=True,
                    )
                    with cluster_config.open_private_atomic_file(pending_path) as stream:
                        view = memoryview(content)
                        while view:
                            written = stream.write(view)
                            if written <= 0:
                                raise RelayError(
                                    "local cleanup report artifact write made no progress"
                                )
                            view = view[written:]
                        stream.flush()
                        os.fsync(stream.fileno())
                else:
                    flags = (
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | getattr(os, "O_BINARY", 0)
                        | getattr(os, "O_NOFOLLOW", 0)
                        | getattr(os, "O_CLOEXEC", 0)
                    )
                    descriptor = (
                        os.open(pending_name, flags, 0o600, dir_fd=directory_fd)
                        if directory_fd is not None
                        else os.open(artifact_directory / pending_name, flags, 0o600)
                    )
                    if os.name == "posix":
                        os.fchmod(descriptor, 0o600)
                    view = memoryview(content)
                    while view:
                        written = os.write(descriptor, view)
                        if written <= 0:
                            raise RelayError("local cleanup report artifact write made no progress")
                        view = view[written:]
                    os.fsync(descriptor)
                    os.close(descriptor)
                    descriptor = None
                fsync_directory()
            staged = read_exact(pending_name, expected_size=len(content), required=True)
            if staged is None or not hmac.compare_digest(staged, content):
                raise RelayError("local cleanup report artifact pending file differs")
            if os.name == "nt":
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                move_file_ex = kernel32.MoveFileExW
                move_file_ex.argtypes = [
                    ctypes.c_wchar_p,
                    ctypes.c_wchar_p,
                    ctypes.c_uint32,
                ]
                move_file_ex.restype = ctypes.c_int
                pending_path = internal_filesystem_path(
                    artifact_directory / pending_name,
                    force_extended=True,
                )
                final_path = internal_filesystem_path(
                    artifact_directory / name,
                    force_extended=True,
                )
                if not move_file_ex(str(pending_path), str(final_path), 0x00000008):
                    error_number = ctypes.get_last_error()
                    raise OSError(
                        error_number,
                        ctypes.FormatError(error_number),
                        str(final_path),
                    )
                fsync_directory()
                committed = read_exact(name, expected_size=len(content), required=True)
                if committed is None or not hmac.compare_digest(committed, content):
                    raise RelayError(
                        "local cleanup report artifact changed after durable publication"
                    )
                return
            publication_complete = False
            try:
                if directory_fd is not None:
                    os.link(
                        pending_name,
                        name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                else:
                    os.link(
                        artifact_directory / pending_name,
                        artifact_directory / name,
                        follow_symlinks=False,
                    )
                fsync_directory()
                final_linked = stat_name(name)
                pending_linked = stat_name(pending_name)
                if not (
                    final_linked is not None
                    and pending_linked is not None
                    and (final_linked.st_dev, final_linked.st_ino)
                    == (pending_linked.st_dev, pending_linked.st_ino)
                    and final_linked.st_nlink == 2
                    and pending_linked.st_nlink == 2
                ):
                    raise RelayError("local cleanup report artifact link publication was not exact")
                linked = read_exact(
                    name,
                    expected_size=len(content),
                    required=True,
                    expected_nlink=2,
                )
                if linked is None or not hmac.compare_digest(linked, content):
                    raise RelayError("local cleanup report artifact linked file differs")
                publication_complete = True
            except FileExistsError:
                raise RelayError(
                    "local cleanup report artifact concurrent publication is ambiguous"
                ) from None
            if publication_complete:
                unlink_name(pending_name)
        except OSError as exc:
            raise RelayError(
                f"local cleanup report artifact cannot be published safely: {exc}"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
        committed = read_exact(name, expected_size=len(content), required=True)
        if committed is None or hashlib.sha256(committed).hexdigest() != expected_sha256:
            raise RelayError("local cleanup report artifact changed after publication")

    try:
        previous_names = newest_previous_complete_report_names(scan_candidates())
        preserved_names = expected_names | previous_names
        prune_unreferenced_candidates(preserve_names=preserved_names)
        chunks: list[cli_cleanup_evidence._LocalCleanupReportChunk] = []
        for chunk_name, chunk, chunk_sha256 in chunk_specs:
            publish_exact(chunk_name, chunk, expected_sha256=chunk_sha256)
            chunks.append(
                cli_cleanup_evidence._LocalCleanupReportChunk(
                    path=artifact_directory / chunk_name,
                    size=len(chunk),
                    sha256=chunk_sha256,
                )
            )
        publish_exact(manifest_name, manifest_payload, expected_sha256=manifest_sha256)
        retained = scan_candidates()
        final_names = {name for name in expected_names if not name.startswith(".")}
        if set(retained) != final_names | previous_names:
            raise RelayError("local cleanup report artifact retention was not exact")
        retained_size = sum(item.st_size for item in retained.values())
        if (
            len(retained) > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_ENTRIES - 1
            or retained_size > MAX_LOCAL_CLEANUP_REPORT_ARTIFACT_STORED_BYTES
        ):
            raise RelayError("local cleanup report artifact retention exceeded its bound")
        verify_directory()
        return cli_cleanup_evidence._LocalCleanupReportArtifact(
            manifest_path=artifact_directory / manifest_name,
            manifest_sha256=manifest_sha256,
            report_sha256=digest,
            report_size=len(payload),
            chunks=tuple(chunks),
        )
    finally:
        if directory_fd is not None:
            os.close(directory_fd)
        if parent_fd is not None:
            os.close(parent_fd)
        try:
            cli_cleanup_evidence._close_windows_pinned_directory(directory_windows_anchor)
        finally:
            try:
                cli_cleanup_evidence._close_windows_pinned_directory(parent_windows_anchor)
            finally:
                release_private_configuration_windows_parent_guard(directory_windows_guard)
