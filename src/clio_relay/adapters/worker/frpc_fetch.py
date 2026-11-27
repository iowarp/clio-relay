"""Fetch a pinned frpc on the site during the worker's sign-in.

The site downloads frpc itself, the way it gets uv, so nothing has to reach
the site outside relay's one ssh pass. Each download is checked against a
sha256 before it is used. Relay pins one frp release per platform; a site's
own ``frpc_url`` and ``frpc_sha256`` replace the pin.
"""

from __future__ import annotations

import shlex

FRP_VERSION = "0.72.0"
_RELEASE = f"https://github.com/fatedier/frp/releases/download/v{FRP_VERSION}"

# (uname -s, uname -m patterns) -> (archive, sha256), from frp's frp_sha256_checksums.txt.
PINNED: dict[tuple[str, str], tuple[str, str]] = {
    ("Linux", "x86_64|amd64"): (
        f"frp_{FRP_VERSION}_linux_amd64.tar.gz",
        "daf719b32ca2778770bb92955e231a07414d754378b9af328aa2642862479804",
    ),
    ("Linux", "aarch64|arm64"): (
        f"frp_{FRP_VERSION}_linux_arm64.tar.gz",
        "02180b857480302fd934e9ddbd50fe2f73229f99062f88756abc6de9df5e95cf",
    ),
    ("Darwin", "arm64"): (
        f"frp_{FRP_VERSION}_darwin_arm64.tar.gz",
        "6e8576c5b6b9914a86850f6ff4fb81c5a09e706f91f70f64d5f398558bdc9db8",
    ),
    ("Darwin", "x86_64"): (
        f"frp_{FRP_VERSION}_darwin_amd64.tar.gz",
        "60439dc6bf794c4182a1a4cac147d642f839d55201607208979eb23aecd09be9",
    ),
}

NO_PIN = (
    "relay has no pinned frpc for this site's platform; set site_frpc (a site path), "
    "frpc_url and frpc_sha256, or frpc_binary"
)


def fetch_script(target: str, *, url: str | None = None, sha256: str | None = None) -> list[str]:
    """Return the bash lines that put a checked frpc at ``target`` on the site.

    With ``url`` and ``sha256`` the site fetches that file; otherwise it picks
    relay's pin for its ``uname``. A ``.tar.gz`` or ``.tgz`` is unpacked for its
    ``frpc``; any other file is taken as the frpc itself. A matching earlier
    download is reused.
    """
    lines: list[str] = []
    if url is not None:
        lines += [f"FRPC_URL={shlex.quote(url)}", f"FRPC_SHA={shlex.quote(sha256 or '')}"]
    else:
        lines.append('case "$(uname -s) $(uname -m)" in')
        for (system, machines), (archive, digest) in PINNED.items():
            pattern = "|".join(f'"{system} {m}"' for m in machines.split("|"))
            lines.append(f'  {pattern}) FRPC_URL="{_RELEASE}/{archive}"; FRPC_SHA={digest} ;;')
        lines += [
            f'  *) echo "{NO_PIN} ($(uname -s) $(uname -m))" >&2; exit 5 ;;',
            "esac",
        ]
    t = '"$FRPC_TMP"'
    lines += [
        f'if [ ! -x "{target}" ] \\',
        f'  || [ "$(cat "{target}.sha256" 2>/dev/null)" != "$FRPC_SHA" ]; then',
        '  FRPC_TMP="$(mktemp -d "$D/dist/frpc.XXXXXX")"',
        f'  if ! curl -fLsS --retry 3 -o {t}/download "$FRPC_URL" >&2; then',
        f'    rm -rf {t}; echo "could not fetch frpc from $FRPC_URL" >&2; exit 5',
        "  fi",
        f'  got="$( (sha256sum {t}/download 2>/dev/null || shasum -a 256 {t}/download) '
        "| cut -d' ' -f1)\"",
        '  if [ "$got" != "$FRPC_SHA" ]; then',
        f"    rm -rf {t}",
        '    echo "frpc from $FRPC_URL has sha256 $got, not the pinned $FRPC_SHA" >&2; exit 5',
        "  fi",
        '  case "$FRPC_URL" in',
        "    *.tar.gz|*.tgz)",
        f"      tar -xzf {t}/download -C {t}",
        '      f=""',
        f'      for c in {t}/frpc {t}/*/frpc; do [ -f "$c" ] && {{ f="$c"; break; }}; done',
        f'      [ -n "$f" ] || {{ rm -rf {t}; echo "no frpc in $FRPC_URL" >&2; exit 5; }} ;;',
        f"    *) f={t}/download ;;",
        "  esac",
        '  chmod 700 "$f"',
        f'  mv -f "$f" "{target}"',
        f'  printf "%s" "$FRPC_SHA" > "{target}.sha256"',
        f"  rm -rf {t}",
        "fi",
    ]
    return lines
