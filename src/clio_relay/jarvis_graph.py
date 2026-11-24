"""Relay's own JARVIS package that collects a site's networks on compute nodes.

jarvis-cd's wheel ships a ``builtin`` repository of packages. Relay never
installs, registers or runs it: the bootstrap removes it from relay's jarvis-cd
environment, and every JARVIS root relay sets up drops it from its repositories
(:data:`DROP_BUILTIN`). So relay's network collection is a package of its own: a
one-package JARVIS repository, :data:`REPO`, that the collect script writes
beside the site graph (:func:`write_lines`) and registers in the site-level
JARVIS root. Its job calls jarvis-cd's own ``ResourceGraphManager.build`` with
``net_only`` on the job's hosts, so the networks are the compute nodes'.
"""

from shlex import quote

REPO = "clio_relay_site"
"""The JARVIS repository of relay's own site packages."""
PACKAGE = "net_graph"
PKG_TYPE = f"{REPO}.{PACKAGE}"
"""What the collect pipeline names (:func:`clio_relay.jarvis_submit.graph_pipeline`)."""

PKG_SOURCE = '''"""Collect the resource graph's net section on the hosts of relay's collect job."""

from jarvis_cd.core.pkg import Application


class NetGraph(Application):
    """jarvis rg build +net_only, run by JARVIS on this job's own nodes."""

    def _init(self):
        pass

    def component_label(self):
        return "network collection"

    def _configure_menu(self):
        return []

    def _configure(self, **kwargs):
        pass

    def start(self):
        from jarvis_cd.core.resource_graph import ResourceGraphManager

        hosts = list(self.get_hostfile().hosts)
        ResourceGraphManager().build(benchmark=False, net_only=True, hosts=hosts)

    def stop(self):
        pass

    def clean(self):
        pass
'''
"""The package's ``pkg.py``."""

DROP_BUILTIN = (
    '! grep -qs builtin "$R/repos.yaml" '
    '|| JARVIS_ROOT="$R" "$JB" repo remove builtin >/dev/null 2>&1 || :\n'
)
"""The shell line that drops jarvis-cd's builtin repository from the JARVIS root ``$R``.

``jarvis init`` lists it in every new root, and a root an earlier relay set up
still has it. ``$JB`` is the ``jarvis`` that owns the root.
"""


def write_lines(site: str) -> str:
    """Shell lines that write :data:`REPO` under ``site`` and register it in ``$R``.

    ``site`` is a shell expression for the directory. It is written each time, so
    a collect always runs this relay's package, and ``+force`` replaces any
    repository of its name.
    """
    package = f"$P/{REPO}/{PACKAGE}"
    return (
        f"P={site}/{REPO}\n"
        f'mkdir -p "{package}" || exit 73\n'
        f': > "$P/{REPO}/__init__.py"; : > "{package}/__init__.py"\n'
        f'printf %s {quote(PKG_SOURCE)} > "{package}/pkg.py" || exit 73\n'
        'JARVIS_ROOT="$R" "$JB" repo add "$P" +force >/dev/null 2>&1 '
        '|| { echo "JARVIS did not register relay\'s network collection package" >&2; exit 70; }\n'
    )
