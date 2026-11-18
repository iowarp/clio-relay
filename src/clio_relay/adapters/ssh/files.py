"""File access on a site over ``ssh`` (the ``files`` verb).

Paths are the site's own: absolute, or relative to the home directory. Each
operation is one ``ssh`` call. Bytes travel on standard input and output, so
nothing is quoted through a shell.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal

from clio_relay.adapters.ssh.scripts import FRESH, parse_sections, q, root_expression
from clio_relay.adapters.ssh.transport import SshTransport
from clio_relay.jobs.file_rules import check_relative, no_delete, whole_writes_only
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import SHA256_PATTERN

_STAT = """\
if stat -c %s / >/dev/null 2>&1; then st() { stat -L -c '%F|%s|%Y|%n' "$@"; }
else st() { stat -L -f '%HT|%z|%m|%N' "$@"; }; fi
"""


def _entry(line: str) -> FileEntry | None:
    parts = line.split("|", 3)
    if len(parts) != 4:
        return None
    kind_text, size_text, mtime_text, path = parts
    lowered = kind_text.lower()
    kind: Literal["file", "dir", "other"] = (
        "dir" if "directory" in lowered else "file" if "regular" in lowered else "other"
    )
    try:
        size = int(size_text)
        modified = datetime.fromtimestamp(int(mtime_text), tz=UTC)
    except ValueError:
        return None
    return FileEntry(path=path, kind=kind, size=size if kind == "file" else None, modified=modified)


def _start(path: str) -> str:
    if not path:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give a path")
    check_relative(path)
    # A job on another node may have just written the path; see scripts.FRESH.
    return (
        'cd "$HOME" 2>/dev/null\n' + f"p={q(path)}\n" + FRESH + 'fresh "$(dirname "$p")"\n' + _STAT
    )


class SshFileAccess:
    """Files on one site, through the system ``ssh``."""

    def __init__(self, transport: SshTransport, *, remote_dir: str, timeout_s: float) -> None:
        self.transport = transport
        self.remote_dir = remote_dir
        self.timeout_s = timeout_s

    def _missing(self, path: str) -> Refusal:
        return Refusal(
            RefusalReason.PATH_NOT_FOUND,
            f"{path} does not exist on {self.transport.config.ssh_profile}",
            path=path,
        )

    async def list(self, path: str) -> list[FileEntry]:
        script = (
            _start(path)
            + '[ -e "$p" ] || { echo "@@missing"; exit 0; }\n'
            + '[ -d "$p" ] || { echo "@@notdir"; exit 0; }\n'
            + 'fresh "$p"\n'
            + 'echo "@@entries"\n'
            + 'for f in "$p"/* "$p"/.[!.]* "$p"/..?*; do\n'
            + '  [ -e "$f" ] || [ -L "$f" ] || continue; st "$f" 2>/dev/null\ndone\n'
        )
        result = await self.transport.ssh(script, timeout_s=self.timeout_s, what="listing")
        sections = parse_sections(result.text())
        if "missing" in sections:
            raise self._missing(path)
        if "notdir" in sections:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"{path} is not a directory", path=path)
        entries = [_entry(line) for line in sections.get("entries", [])]
        return sorted((entry for entry in entries if entry), key=lambda entry: entry.path)

    async def stat(self, path: str) -> FileEntry:
        script = (
            _start(path)
            + '[ -e "$p" ] || { echo "@@missing"; exit 0; }\n'
            + '[ -f "$p" ] && : < "$p" 2>/dev/null\n'  # opening it refreshes its size
            + 'echo "@@entry"; st "$p"\n'
        )
        result = await self.transport.ssh(script, timeout_s=self.timeout_s, what="stat")
        sections = parse_sections(result.text())
        entry = next((_entry(line) for line in sections.get("entry", []) if line), None)
        if "missing" in sections or entry is None:
            raise self._missing(path)
        return entry

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        if offset < 0 or (limit is not None and limit < 0):
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "offset and limit are not negative")
        check_relative(path)
        if limit == 0:
            await self.stat(path)
            return b""
        command = f'tail -c +{offset + 1} "$p"' if offset else 'cat "$p"'
        if limit is not None:
            command += f" | head -c {limit}"
        script = (
            'cd "$HOME" 2>/dev/null\n'
            + f"p={q(path)}\n"
            + FRESH
            + 'fresh "$(dirname "$p")"\n'
            + '[ -f "$p" ] || { echo "missing" >&2; exit 44; }\n'
            + f"{command}\n"
        )
        result = await self.transport.ssh(script, timeout_s=None, what="reading")
        if result.exit_code == 44:
            raise self._missing(path)
        if result.exit_code != 0:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"could not read {path}: {result.error_text().strip()}",
                path=path,
            )
        return result.stdout

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        whole_writes_only(path, offset, final)
        script = (
            _start(path)
            + f"ow={1 if overwrite else 0}\n"
            + 'if [ -e "$p" ] && [ "$ow" = 0 ]; then cat >/dev/null; echo "@@exists"; exit 0; fi\n'
            + 'd=$(dirname "$p"); mkdir -p "$d" || exit 45\n'
            + 't="$d/.relay-write.$$"\n'
            + 'cat > "$t" && mv -f "$t" "$p" || { rm -f "$t"; exit 45; }\n'
            + 'echo "@@entry"; st "$p"\n'
        )
        result = await self.transport.ssh(script, data=data, timeout_s=None, what="writing")
        sections = parse_sections(result.text())
        if "exists" in sections:
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
            )
        entry = next((_entry(line) for line in sections.get("entry", []) if line), None)
        if result.exit_code != 0 or entry is None:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"could not write {path}: {result.error_text().strip()}",
                path=path,
            )
        return entry

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        raise no_delete(path)

    def _cas(self, digest: str) -> str:
        digest = digest.lower()
        if not SHA256_PATTERN.match(digest):
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "not a sha256 digest", digest=digest)
        return f'"$R"/content/cas/sha256/{digest[:2]}/{digest}'

    async def stage(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        script = (
            f"R={root_expression(self.remote_dir)}\n"
            + f"c={self._cas(digest)}\n"
            + FRESH
            + 'fresh "${c%/*}"\n'
            + 'if [ -f "$c" ]; then cat >/dev/null; echo "@@have"; exit 0; fi\n'
            + 'mkdir -p "$(dirname "$c")" || exit 45\n'
            + 't="$c.$$"; cat > "$t" || { rm -f "$t"; exit 45; }\n'
            + "if command -v sha256sum >/dev/null 2>&1; then "
            + "got=$(sha256sum \"$t\" | cut -d' ' -f1); "
            + f'[ "$got" = {digest} ] || {{ rm -f "$t"; exit 46; }}; fi\n'
            + 'chmod 444 "$t"; mv -f "$t" "$c"; echo "@@stored"\n'
        )
        result = await self.transport.ssh(script, data=data, timeout_s=None, what="staging")
        if result.exit_code != 0:
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"could not stage content: {result.error_text().strip()}",
                digest=digest,
            )
        return digest

    async def has(self, digest: str) -> bool:
        script = (
            f"R={root_expression(self.remote_dir)}\n"
            + f"c={self._cas(digest)}\n"
            + FRESH
            + 'fresh "${c%/*}"\n'
            + '[ -f "$c" ] && echo "@@have"\nexit 0\n'
        )
        result = await self.transport.ssh(script, timeout_s=self.timeout_s, what="checking")
        return "have" in parse_sections(result.text())
