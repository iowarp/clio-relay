"""Scratch directories a sandboxed relay can still use.

Since Python 3.13, ``tempfile.mkdtemp`` on Windows calls ``os.mkdir(path, 0o700)``, which
writes an owner-only DACL (SYSTEM, Administrators, OWNER RIGHTS). A process in an AppContainer
needs its container SID in the DACL too, so it cannot use the directory it just made. Here the
directory is made with the default mode on Windows, so it inherits its parent's ACL (the
per-user or per-container temp folder), as it did before 3.13. POSIX keeps 0700.
"""

from __future__ import annotations

import os
import secrets
import shutil
import stat
import tempfile
import weakref
from pathlib import Path
from types import TracebackType

_ATTEMPTS = 100


def make_dir(prefix: str = "clio-relay-", parent: str | os.PathLike[str] | None = None) -> str:
    """Make a fresh, uniquely named directory under ``parent`` (default: the temp folder)."""
    base = os.fspath(parent) if parent is not None else tempfile.gettempdir()
    for _ in range(_ATTEMPTS):
        path = os.path.join(base, f"{prefix}{secrets.token_hex(6)}")
        try:
            if os.name == "nt":
                os.mkdir(path)
            else:
                os.mkdir(path, 0o700)
        except FileExistsError:
            continue
        return os.path.abspath(path)
    raise FileExistsError(f"no free scratch directory name under {base} with prefix {prefix!r}")


def _writable_retry(function: object, path: str, error: BaseException) -> None:
    """Clear a read-only bit (git objects on Windows) and try the removal once more."""
    del function
    if not isinstance(error, PermissionError):
        raise error
    os.chmod(path, stat.S_IWRITE)
    if os.path.isdir(path) and not os.path.islink(path):
        os.rmdir(path)
    else:
        os.unlink(path)


def remove(path: str | os.PathLike[str], *, ignore_errors: bool = False) -> None:
    """Remove a scratch tree, read-only files included."""
    try:
        shutil.rmtree(path, onexc=_writable_retry)
    except FileNotFoundError:
        return
    except OSError:
        if not ignore_errors:
            raise


class ScratchDir:
    """A scratch directory with ``tempfile.TemporaryDirectory``'s lifetime.

    ``name`` is the path; ``cleanup()`` removes it and raises on failure unless
    ``ignore_cleanup_errors``; the context manager yields ``name``. A directory never cleaned
    up is removed when the object is collected.
    """

    def __init__(
        self,
        prefix: str = "clio-relay-",
        parent: str | os.PathLike[str] | None = None,
        *,
        ignore_cleanup_errors: bool = False,
    ) -> None:
        self.name = make_dir(prefix, parent)
        self._ignore_cleanup_errors = ignore_cleanup_errors
        self._finalizer = weakref.finalize(self, remove, self.name, ignore_errors=True)

    @property
    def path(self) -> Path:
        return Path(self.name)

    def cleanup(self) -> None:
        if self._finalizer.detach() is not None or os.path.exists(self.name):
            remove(self.name, ignore_errors=self._ignore_cleanup_errors)

    def __enter__(self) -> str:
        return self.name

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.cleanup()

    def __repr__(self) -> str:
        return f"<ScratchDir {self.name!r}>"
