"""File access on a site over ``ssh`` (the ``files`` verb, docs/files.md).

An absolute path is the site's own, ``~/`` is the home directory, and a
relative path is under ``remote_dir``, relay's work directory. Each operation
is one ``ssh`` call. Bytes travel on standard input and output, so nothing is
quoted through a shell.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal

from clio_relay.adapters.ssh.scripts import FRESH, q, root_expression
from clio_relay.adapters.ssh.transport import SshTransport
from clio_relay.jarvis_remote import parse_sections
from clio_relay.jobs.file_rules import (
    check_offset,
    check_relative,
    not_empty_refusal,
    protected_refusal,
    resume_refusal,
)
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import SHA256_PATTERN

_STAT = """\
if stat -c %s / >/dev/null 2>&1; then st() { stat -L -c '%F|%s|%Y|%n' "$@"; }
else st() { stat -L -f '%HT|%z|%m|%N' "$@"; }; fi
"""

_RESOLVE = """\
case "$p" in
  /*) ;;
  "~") p=$HOME ;;
  "~/"*) p=$HOME/${p#"~/"} ;;
  "") p=$R ;;
  *) p=$R/$p ;;
esac
"""

# A chunk lands in a temporary file first, so a stream cut short appends
# nothing. A chunk sent again after its answer was lost is recognised by the
# part's size and the hash of its tail, so a retry is safe.
_CHUNK = """\
size() { if [ -f "$1" ]; then : < "$1"; wc -c < "$1" | tr -d ' '; else echo 0; fi; }
tailsum() {
  tail -c "$n" "$1" | if command -v sha256sum >/dev/null 2>&1; then sha256sum
  else shasum -a 256; fi 2>/dev/null | cut -d' ' -f1
}
part="$p.relay-part"; d=$(dirname "$p")
mkdir -p "$d" || { cat >/dev/null; exit 45; }
t="$d/.relay-chunk.$$"
cat > "$t" || { rm -f "$t"; exit 45; }
[ "$(wc -c < "$t" | tr -d ' ')" = "$n" ] || { rm -f "$t"; echo "@@short"; exit 0; }
if [ "$off" = 0 ]; then
  if [ -e "$p" ] && [ "$ow" = 0 ]; then rm -f "$t"; echo "@@exists"; exit 0; fi
  mv -f "$t" "$part" || { rm -f "$t"; exit 45; }
else
  held=$(size "$part")
  if [ "$held" = "$off" ]; then
    cat "$t" >> "$part" || { rm -f "$t"; exit 45; }
  elif [ "$held" = $((off + n)) ] && [ "$(tailsum "$part")" = "$h" ]; then :
  elif [ "$fin" = 1 ] && [ ! -e "$part" ] && [ "$(size "$p")" = $((off + n)) ] \\
      && [ "$(tailsum "$p")" = "$h" ]; then
    rm -f "$t"; echo "@@entry"; st "$p"; exit 0
  else rm -f "$t"; printf '%s\\n' "@@resume $held"; exit 0
  fi
  rm -f "$t"
fi
if [ "$fin" = 0 ]; then echo "@@entry"; st "$part"; exit 0; fi
if [ -e "$p" ] && [ "$ow" = 0 ]; then echo "@@exists"; exit 0; fi
mv -f "$part" "$p" || exit 45
echo "@@entry"; st "$p"
"""

_DELETE = """\
if [ -L "$p" ]; then rm -f "$p" || exit 45; echo "@@done"; exit 0; fi
[ -e "$p" ] || { echo "@@missing"; exit 0; }
if [ -d "$p" ]; then
  real=$(cd "$p" && pwd -P)
  for keep in "$R" "$HOME" /; do
    k=$(cd "$keep" 2>/dev/null && pwd -P)
    if [ -n "$k" ] && [ "$real" = "$k" ]; then echo "@@protected"; exit 0; fi
  done
  if [ "$rec" = 1 ]; then rm -rf "$p" || exit 45
  else
    fresh "$p"
    rmdir "$p" 2>/dev/null || { [ -n "$(ls -A "$p")" ] && echo "@@notempty"; exit 0; }
  fi
else rm -f "$p" || exit 45
fi
echo "@@done"
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


class SshFileAccess:
    """Files on one site, through the system ``ssh``."""

    transfer_chunk_bytes = 32 * 1024 * 1024
    """Each chunk is one ``ssh`` call, so a path transfer moves large ones."""

    def __init__(self, transport: SshTransport, *, remote_dir: str, timeout_s: float) -> None:
        self.transport = transport
        self.remote_dir = remote_dir
        self.timeout_s = timeout_s

    def _start(self, path: str, *, empty: bool = False) -> str:
        """Set ``R`` to the work directory and ``p`` to ``path`` on the site."""
        if not path and not empty:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give a path")
        if path.startswith("~") and path != "~" and not path.startswith("~/"):
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                "only your own home is reachable as ~/; give another user's path in full",
                path=path,
            )
        check_relative(path)
        # A job on another node may have just written the path; see scripts.FRESH.
        return (
            'cd "$HOME" 2>/dev/null\n'
            + f"R={root_expression(self.remote_dir)}\n"
            + f"p={q(path)}\n"
            + _RESOLVE
            + FRESH
            + 'fresh "$(dirname "$p")"\n'
            + _STAT
        )

    def _missing(self, path: str) -> Refusal:
        return Refusal(
            RefusalReason.PATH_NOT_FOUND,
            f"{path} does not exist on {self.transport.title}",
            path=path,
        )

    def _failed(self, doing: str, path: str, error: str) -> Refusal:
        return Refusal(
            RefusalReason.UNREACHABLE, f"could not {doing} {path}: {error.strip()}", path=path
        )

    async def list(self, path: str) -> list[FileEntry]:
        script = (
            self._start(path, empty=True)
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
            self._start(path, empty=True)
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
        if limit == 0:
            await self.stat(path)
            return b""
        command = f'tail -c +{offset + 1} "$p"' if offset else 'cat "$p"'
        if limit is not None:
            command += f" | head -c {limit}"
        script = (
            self._start(path) + '[ -f "$p" ] || { echo "missing" >&2; exit 44; }\n' + f"{command}\n"
        )
        result = await self.transport.ssh(script, timeout_s=None, what="reading")
        if result.exit_code == 44:
            raise self._missing(path)
        if result.exit_code != 0:
            raise self._failed("read", path, result.error_text())
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
        check_offset(path, offset)
        if offset == 0 and final:
            return await self._write_whole(path, data, overwrite=overwrite)
        script = (
            self._start(path)
            + f"ow={1 if overwrite else 0}; fin={1 if final else 0}\n"
            + f"off={offset}; n={len(data)}; h={hashlib.sha256(data).hexdigest()}\n"
            + _CHUNK
        )
        result = await self.transport.ssh(script, data=data, timeout_s=None, what="writing")
        sections = parse_sections(result.text())
        if "exists" in sections:
            raise self._exists(path)
        resume = sections.get("resume")
        if resume:
            raise resume_refusal(path, int(resume[0]))
        if "short" in sections:
            raise self._failed("write", path, "the chunk arrived short; send it again")
        entry = next((_entry(line) for line in sections.get("entry", []) if line), None)
        if result.exit_code != 0 or entry is None:
            raise self._failed("write", path, result.error_text())
        return entry

    def _exists(self, path: str) -> Refusal:
        return Refusal(
            RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
        )

    async def _write_whole(self, path: str, data: bytes, *, overwrite: bool) -> FileEntry:
        script = (
            self._start(path)
            + f"ow={1 if overwrite else 0}\n"
            + 'if [ -e "$p" ] && [ "$ow" = 0 ]; then cat >/dev/null; echo "@@exists"; exit 0; fi\n'
            + 'd=$(dirname "$p"); mkdir -p "$d" || exit 45\n'
            + 't="$d/.relay-write.$$"\n'
            + 'cat > "$t" && mv -f "$t" "$p" || { rm -f "$t"; exit 45; }\n'
            + 'echo "@@entry"; st "$p"\n'
        )
        # Without overwrite, a second try would find the first one's file and refuse.
        result = await self.transport.ssh(
            script, data=data, timeout_s=None, what="writing", retry=overwrite
        )
        sections = parse_sections(result.text())
        if "exists" in sections:
            raise self._exists(path)
        entry = next((_entry(line) for line in sections.get("entry", []) if line), None)
        if result.exit_code != 0 or entry is None:
            raise self._failed("write", path, result.error_text())
        return entry

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        script = self._start(path) + f"rec={1 if recursive else 0}\n" + _DELETE
        # A second try after a delete that went through would find nothing and refuse.
        result = await self.transport.ssh(script, timeout_s=None, what="deleting", retry=False)
        sections = parse_sections(result.text())
        if "missing" in sections:
            raise self._missing(path)
        if "protected" in sections:
            raise protected_refusal(path)
        if "notempty" in sections:
            raise not_empty_refusal(path)
        if result.exit_code != 0 or "done" not in sections:
            raise self._failed("delete", path, result.error_text() or "permission denied")

    async def sha256(self, path: str) -> str | None:
        """Hash ``path`` on the site, or return ``None`` when it has no sha256 tool."""
        script = (
            self._start(path)
            + '[ -f "$p" ] || { echo "@@missing"; exit 0; }\n'
            + 'if command -v sha256sum >/dev/null 2>&1; then s=$(sha256sum < "$p")\n'
            + 'elif command -v shasum >/dev/null 2>&1; then s=$(shasum -a 256 < "$p")\n'
            + 'else echo "@@none"; exit 0; fi\n'
            + 'echo "@@sum"; printf "%s\\n" "${s%% *}"\n'
        )
        result = await self.transport.ssh(script, timeout_s=None, what="hashing")
        sections = parse_sections(result.text())
        if "missing" in sections:
            raise self._missing(path)
        found = next((line.strip() for line in sections.get("sum", []) if line.strip()), None)
        if result.exit_code != 0 or (found is None and "none" not in sections):
            raise self._failed("hash", path, result.error_text())
        return found if found and SHA256_PATTERN.match(found.lower()) else None

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
