"""Relay's own JARVIS package that collects a site's networks on compute nodes.

jarvis-cd's wheel ships a ``builtin`` repository of packages. Relay never
registers or runs it: every JARVIS call relay makes sets :data:`NO_BUILTIN_ENV`,
and every JARVIS root relay sets up records ``no_builtin: true`` at ``init``, so
JARVIS never lists or resolves builtin. Relay's network collection is a
package of its own: a
one-package JARVIS repository, :data:`REPO`, that the collect script writes
beside the site graph (:func:`write_lines`) and registers in the site-level
JARVIS root. Its job calls jarvis-cd's own ``ResourceGraphManager.build`` with
``net_only`` on the job's hosts, so the networks are the compute nodes'.
"""

from shlex import quote

NO_BUILTIN_ENV = "JARVIS_NO_BUILTIN"
"""jarvis-cd's switch (1.9.1 and later) that leaves its builtin repository out.

Set to 1 on a JARVIS call, builtin is never registered or resolved and ``builtin.*``
is not found. Set at ``jarvis init``, the root records ``no_builtin: true``
(:data:`NO_BUILTIN_CONFIG`), so JARVIS's own jobs in that root leave it out too.
"""
NO_BUILTIN_CONFIG = "no_builtin: true"
"""The line ``jarvis init`` writes in ``jarvis_config.yaml`` with :data:`NO_BUILTIN_ENV` set."""
NO_BUILTIN = f"export {NO_BUILTIN_ENV}=1\n"
"""The shell line that sets :data:`NO_BUILTIN_ENV` for the rest of a script."""

REPO = "clio_relay_site"
"""The JARVIS repository of relay's own site packages."""
PACKAGE = "net_graph"
PKG_TYPE = f"{REPO}.{PACKAGE}"
"""What the collect pipeline names (:func:`clio_relay.jarvis_submit.graph_pipeline`)."""
LABEL = "network collection"
"""The package's ``component_label``, as relay's card names it before JARVIS runs."""

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
