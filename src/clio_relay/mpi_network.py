"""Which network an MPI job's ranks talk over, on a Slurm site.

Open MPI's TCP transport (the ``tcp`` BTL, and PRRTE's out-of-band channel)
uses every interface it finds unless told otherwise. On a site whose compute
nodes share an address that is not a real link between them (``docker0`` at
``172.17.0.1`` on every node, a link-local USB NIC), ranks on different nodes
dial that address, reach their own node, and the job dies at startup
(``btl_tcp_endpoint.c: received unexpected process identifier``). One-node runs
use shared memory and never see it.

Relay names the network for the job, from what the site's own configuration
says: the network that holds Slurm's node addresses (``sinfo -N -o %o``),
which is how the scheduler and ``ssh`` reach the nodes, as the login node's own
interfaces give its prefix. :data:`SHELL` discovers it on the login node with
short reads. The site script (:data:`clio_relay.jarvis_submit.SITE_SCRIPT`)
keeps the answer in the CLIO site home, so every job on the site uses the same
one, and exports :func:`launch_env` before it hands the pipeline to JARVIS. The
variables reach the ranks the way any submit-time environment does: Slurm's
``sbatch`` exports the submitting environment (``--export=ALL``, its default),
and Open MPI's launcher forwards every ``OMPI_MCA_*`` variable to each rank.
JARVIS's own pipeline environment does not list them.
A job whose own environment sets one of :data:`KEYS` (or an ``_if_exclude``)
keeps its own. On a guarded site, where a package job takes no environment,
the person names another fabric by changing the site's record
(:func:`site_file`), which every later job reads.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

FILE = "mpi-network"
"""The file under the CLIO site home's JARVIS directory that keeps the site's network."""

SITE_DIR = "jarvis-cd"
"""The CLIO site home's JARVIS directory (:data:`clio_relay.jarvis_submit.BOOTSTRAP_DIR`)."""

JOB_FILE = "relay.mpi-network"
"""The job directory's copy: the network the job's launch used, and where it came from."""

KEYS = (
    "OMPI_MCA_btl_tcp_if_include",
    "OMPI_MCA_oob_tcp_if_include",
    "PRTE_MCA_oob_tcp_if_include",
)
"""The Open MPI and PRRTE settings that name the interfaces their TCP channels use."""

_PY = (
    "import ipaddress, socket, sys\n"
    "found = []\n"
    "for name in sys.argv[1].split():\n"
    "    try:\n"
    "        found.append(ipaddress.ip_address(socket.gethostbyname(name)))\n"
    "    except (OSError, ValueError):\n"
    "        pass\n"
    "best = None\n"
    "for text in sys.argv[2].split():\n"
    "    try:\n"
    "        net = ipaddress.ip_interface(text).network\n"
    "    except ValueError:\n"
    "        continue\n"
    "    if not found or net.is_loopback or net.is_link_local:\n"
    "        continue\n"
    "    if all(a in net for a in found) and (best is None or net.prefixlen > best.prefixlen):\n"
    "        best = net\n"
    "if best is not None:\n"
    "    print(best, len(found))\n"
)

SHELL = (
    "relay_mpi_network() {\n"
    "  command -v sinfo >/dev/null 2>&1 || return 0\n"
    "  addrs=$(sinfo -h -N -o '%o' 2>/dev/null | sort -u | head -n 256 | tr '\\n' ' ')\n"
    '  [ -n "$addrs" ] || return 0\n'
    "  ipc=$(command -v ip 2>/dev/null) || ipc=\n"
    '  [ -n "$ipc" ] || for c in /usr/sbin/ip /sbin/ip; do [ -x "$c" ] && ipc=$c && break; done\n'
    '  [ -n "$ipc" ] || return 0\n'
    "  nets=$(\"$ipc\" -o -4 addr show 2>/dev/null | awk '{print $4}' | tr '\\n' ' ')\n"
    '  [ -n "$nets" ] || return 0\n'
    f'  "$1" -c \'{_PY}\' "$addrs" "$nets" 2>/dev/null || true\n'
    "}\n"
)
"""Shell that defines ``relay_mpi_network <python>``.

It prints ``<cidr> <nodes>``: the most specific login-node network that holds
every Slurm node address, and how many addresses it checked. It prints nothing
on a site without Slurm, ``ip`` or such a network.
"""


def launch_env(cidr: str) -> dict[str, str]:
    """Return the launch settings that keep Open MPI's TCP channels on ``cidr``."""
    return dict.fromkeys(KEYS, cidr)


def facts(lines: list[str]) -> dict[str, Any] | None:
    """Return the site's MPI network from :data:`SHELL`'s line, for ``describe``."""
    for line in lines:
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            return {
                "network": parts[0],
                "node_addresses": int(parts[1]),
                "source": "the network of Slurm's node addresses",
                "launch_env": launch_env(parts[0]),
            }
    return None


_SIGNS = (
    "received unexpected process identifier",
    "btl_tcp_endpoint",
    "btl_tcp: connect",
    "unable to reliably start",
    "lost communication with a remote daemon",
    "has lost communication",
    "no route to host",
    "the tcp btl failed",
)


def site_file(root: str) -> str:
    """Return the site's MPI network record under the CLIO site home ``root``."""
    return f"{root.rstrip('/')}/{SITE_DIR}/{FILE}"


def failure_hint(
    text: str, used: Mapping[str, str] | None = None, root: str | None = None
) -> dict[str, Any] | None:
    """Return what to try when a failed run's output looks like MPI used the wrong network.

    ``used`` is the job's launch settings, when relay knows them. ``root`` is
    the CLIO site home, so the hint names the site's record to change.
    """
    lowered = text.lower()
    seen = [sign for sign in _SIGNS if sign in lowered]
    if not seen:
        return None
    current = dict(used or {})
    network = current.get(KEYS[0])
    if network:
        said = f"this run's ranks used {network} ({KEYS[0]})"
    elif used is not None:
        said = "this run named no network for its ranks"
    else:
        said = f"the job directory's {JOB_FILE} says which network this run named"
    record = site_file(root) if root else f"<CLIO site home>/{SITE_DIR}/{FILE}"
    hint: dict[str, Any] = {
        "kind": "mpi_network",
        "matched": seen,
        "launch_env": current,
        "site_record": record,
        "message": (
            "The run's MPI ranks could not reach each other over TCP: Open MPI likely "
            f"picked an interface that is not a real link between the nodes; {said}. "
            f"Every job on the site uses the network in {record} (first word: a CIDR, "
            "interface names joined by commas, or none). describe(site) shows the network "
            "relay found and the nodes' interfaces can be read with exec (ip -o -4 addr). "
            "Write the nodes' fabric there with files (op write, overwrite true; a guarded "
            "site asks the person first), as one line '<network> person', and run again. "
            f"On a site that takes job environment variables, {', '.join(KEYS)} in the job's "
            "environment name it for one job. site clean drops the record, and relay finds "
            "the network again."
        ),
    }
    return hint
