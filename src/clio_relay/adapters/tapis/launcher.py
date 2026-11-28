"""Relay's launcher archive and the job record it reads (RL47)."""

from __future__ import annotations

import base64
import hashlib
import io
import tarfile
from functools import cache
from importlib.resources import files

from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.jobs.spec import JobSpec
from clio_relay.stable_gzip import stable_gzip

LAUNCHER_FORMAT = "2"
SCRIPT_NAME = "tapisjob_app.sh"


@cache
def script() -> bytes:
    """Return the launcher script, with Unix line ends whatever the checkout did."""
    raw = files(__package__ or "clio_relay.adapters.tapis").joinpath(SCRIPT_NAME).read_bytes()
    return raw.replace(b"\r\n", b"\n")


@cache
def archive() -> bytes:
    """Return the launcher as a gzip tar. The bytes depend only on the script."""
    body = script()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        info = tarfile.TarInfo(SCRIPT_NAME)
        info.size = len(body)
        info.mode = 0o755
        info.mtime = 0
        tar.addfile(info, io.BytesIO(body))
    return stable_gzip(buffer.getvalue())


@cache
def version() -> str:
    """The app version: the record format and the launcher's own digest."""
    return f"{LAUNCHER_FORMAT}.0-{hashlib.sha256(script()).hexdigest()[:10]}"


def archive_name() -> str:
    return f"clio-relay-launcher-{version()}.tar.gz"


def encode(fields: list[str]) -> str:
    """Pack fields into one shell-safe argument."""
    for item in fields:
        if "\x00" in item:
            raise Refusal(RefusalReason.INVALID_SPEC, "a job field holds a NUL character")
    raw = b"".join(item.encode("utf-8") + b"\x00" for item in fields)
    return base64.b64encode(raw).decode("ascii")


def decode(arg: str) -> list[str]:
    """Unpack :func:`encode`'s output. Used by tests and the stub."""
    raw = base64.b64decode(arg)
    return [part.decode("utf-8") for part in raw.split(b"\x00")[:-1]]


def record(spec: JobSpec, *, content_dir: str) -> list[str]:
    """Build the launcher's fields for ``spec``, a job with an executable."""
    head = [
        LAUNCHER_FORMAT,
        spec.directory or "",
        spec.stdin_path or "",
        spec.stdout_path or "",
        spec.stderr_path or "",
        content_dir,
        str(len(spec.inputs)),
    ]
    for item in spec.inputs:
        head += [item.name, item.sha256]
    assert spec.executable is not None
    return head + [spec.executable, *spec.arguments]
