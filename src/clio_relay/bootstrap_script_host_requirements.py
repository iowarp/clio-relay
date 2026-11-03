"""Rendered-script fragment: refuse an unusable host before any stage relies on it.

The cold bootstrap runs its verification stages under the host's own ``python3``
and, on the retained-state reconcile path, copies an already-present
``~/.local/bin/uv``. Both are host facts the script cannot fix silently, so it
checks them up front and refuses with a message that names what it found, what
it needs, and how to fix it, rather than dying later in an untyped traceback
(iowarp/clio-relay#283).
"""

from __future__ import annotations

from clio_relay.bootstrap_constants import UV_LINUX_AMD64_EXECUTABLE_SHA256, UV_VERSION

#: Oldest host ``python3`` the bootstrap's own stages run under. The bundled
#: candidate sources use ``@dataclass(slots=True)`` (``safe_archive.py``) and
#: inline programs use ``X | None`` at runtime, both 3.10+. The relay runtime
#: itself installs its own pinned interpreter via uv, so this is only the floor
#: for the verification stages.
BOOTSTRAP_MIN_SYSTEM_PYTHON = (3, 10)

UV_LINUX_AMD64_ARCHIVE_URL = (
    f"https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/"
    "uv-x86_64-unknown-linux-gnu.tar.gz"
)


def host_requirements_script() -> str:
    """Render the python3 floor check and the pinned-uv source check function."""
    major, minor = BOOTSTRAP_MIN_SYSTEM_PYTHON
    floor = f"{major}.{minor}"
    # The probe must run (and report) under any python3, including Python 2,
    # so it uses only syntax every version accepts.
    probe = (
        "import sys; v = sys.version_info; "
        "sys.stdout.write('%d.%d.%d' % (v[0], v[1], v[2])); "
        f"sys.exit(0 if (v[0], v[1]) >= ({major}, {minor}) else 1)"
    )
    python_need = f"bootstrap requires python3 >= {floor} on PATH for its verification stages"
    python_fix = (
        "These stages need only a standard-library Python (the relay runtime installs "
        f"its own pinned interpreter). Put a python3 >= {floor} first on PATH "
        "(\\$HOME/.local/bin is searched first) and re-run bootstrap."
    )
    found_python = (
        "${BOOTSTRAP_SYSTEM_PYTHON_VERSION:-an interpreter that could not report its version}"
    )
    uv_need = f"retained-state reconcile requires the pinned uv {UV_VERSION}"
    uv_fix = (
        f"Extract uv and uvx from {UV_LINUX_AMD64_ARCHIVE_URL} into \\$HOME/.local/bin "
        "and re-run bootstrap."
    )
    found_uv = (
        "${observed_version:-a uv that did not report its version} "
        "(sha256 ${observed_sha256:-unreadable})"
    )
    uv_absent = f"{uv_need} at $source, but nothing is there, and this path does not install uv."
    pinned_uv = f"{uv_need} (executable sha256 {UV_LINUX_AMD64_EXECUTABLE_SHA256})"
    return f"""BOOTSTRAP_SYSTEM_PYTHON="$(command -v python3 || true)"
if [ -z "$BOOTSTRAP_SYSTEM_PYTHON" ]; then
  echo "{python_need}; none was found. {python_fix}" >&2
  exit 1
fi
if ! BOOTSTRAP_SYSTEM_PYTHON_VERSION="$(python3 -c "{probe}" 2>/dev/null)"; then
  echo "{python_need}; found {found_python} at $BOOTSTRAP_SYSTEM_PYTHON. {python_fix}" >&2
  exit 1
fi
bootstrap_require_pinned_uv_source() {{
  local source="$1" observed_sha256 observed_version
  if [ ! -e "$source" ] && [ ! -L "$source" ]; then
    echo "{uv_absent} {uv_fix}" >&2
    exit 1
  fi
  observed_sha256="$(sha256sum < "$source" 2>/dev/null | cut -d ' ' -f 1 || true)"
  if [ "$observed_sha256" != "{UV_LINUX_AMD64_EXECUTABLE_SHA256}" ]; then
    observed_version="$(timeout 10 "$source" --version 2>/dev/null | head -n 1 || true)"
    echo "{pinned_uv} at $source, but found {found_uv}. {uv_fix}" >&2
    exit 1
  fi
}}
"""
