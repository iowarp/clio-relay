# The one-link control plane

The relay transport design is a single persistent link between the local relay
process and each remote relay it is connected to. The link is established once,
at connection bring-up, and every owned-session operation rides it as plain HTTP
against the mapped port.

This document describes the implemented shape, the transport modes, and the
two-sided measurement the deployment gate re-runs.

## Modes

| Mode | Bring-up | Runtime transport connections |
| --- | --- | --- |
| `brokered_tcp` | one SSH to deploy the cluster relay (skipped when already deployed); both relays dial out to an internet-accessible server, which brokers the handshake | 0 |
| `udp_rendezvous` | same rendezvous, UDP hole-punching handshake, falling back to `brokered_tcp`'s server-carried TCP | 0 |
| `ssh_forward` | one SSH to deploy (skipped when already deployed); one SSH holding the port forward, which the present user authorizes | 1 |

Only `ssh_forward` is implemented. `brokered_tcp` and `udp_rendezvous` are
declared and slot in as sibling implementations of `RelayTransport`
(`src/clio_relay/control_channel.py`). Asking for one today raises
`TransportModeUnavailable` — an unbuilt mode must never quietly degrade into
per-operation SSH.

## How bring-up spends exactly one connection

In `ssh_forward` mode a single SSH process does both jobs:

```
ssh -T -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 \
    -L 127.0.0.1:<local>:127.0.0.1:<remote_api_port> <ssh_host> bash -lc '<bootstrap>'
```

The `<bootstrap>` command runs the two cluster-local executors that used to be
separate `ssh ... bash -s` dials — `session recovery-status` and
`session challenge-owned` — prints their exact output as one JSON line, and then
blocks on stdin (`exec cat >/dev/null`). Closing the local end of that pipe is
how the channel is torn down.

Carrying the identity challenge on the bring-up command is not an optimization,
it is required: the owner token that signs the identity document is minted
cluster-side and never leaves the cluster, so the local relay cannot compute the
expected document itself. The proof therefore has to arrive over the same
authenticated act that establishes the channel.

`BatchMode` is deliberately **not** set for bring-up: the user is present to
approve exactly this one connection's two-factor prompt. A non-interactive
deployment can opt out with `allow_interactive_authorization=False`, which adds
`BatchMode=yes` and makes an unattended dial fail rather than hang.

## Configuration

The forward has to be pointed at the remote owned-session API port before the
channel exists, so the port is connection configuration rather than a
per-operation discovery:

1. an explicit `remote_api_port` argument,
2. `CLIO_RELAY_OWNER_SESSION_API_PORT`,
3. the default, `8765`.

Whatever is resolved is cross-checked against the remote relay's own report in
the bring-up document. A wrong port fails with a typed error naming both values
instead of binding to a stranger.

## Reconnect

Reconnect is explicit. If the held channel drops, the next operation raises
`ChannelDropped` and records a typed `dropped` event; it never redials. Calling
`RemoteConnection.reconnect()` opens exactly one new transport and records
`reestablishing` → `authorization_required` → `establishing` → `reestablished`.
In `ssh_forward` mode that call is what the user authorizes.

`RemoteConnectionRegistry.reconnect(cluster)` is the single entry point for it,
so an operator action can reach it and a retry cannot.

A broken *TCP stream* is not a broken *channel*. HTTP streams are pooled over
the one held channel — a long poll on one operation must not block every other
operation on the same cluster — and opening another stream is another TCP
connection *through the forward that is already held*, so it costs no new
transport. Each stream is proven against the same out-of-band bring-up identity
document before any credential is sent, and extra streams are recorded as
`stream_reproven`.

## One local relay, many remote relays

`RemoteConnectionRegistry` maps each cluster to its own held connection. The
client-facing MCP endpoint stays single and stable while connections come and
go: connecting, disconnecting, or reconnecting one cluster leaves every other
cluster's channel untouched.

## The two-sided acceptance measurement

The gate measures the same quantity from both ends and requires them to agree.

**Client side (desktop).** The local relay's own typed record:

```python
from clio_relay.remote_connection import connection_registry

report = connection_registry().event_report()
report["transport_connections_opened"]   # established + reestablished, all clusters
report["clusters"]["<cluster>"]["events"]  # the full typed lifecycle record
```

Out of process, the desktop sampler counts live `ssh` children of the door
process across the window under test (the `ssh-sampler.ps1` shape used during
the 1.5.10 measurement):

```powershell
Get-CimInstance Win32_Process -Filter "Name='ssh.exe'" |
  Select-Object ProcessId, CreationDate, CommandLine
```

Sample at a fixed interval for the duration of the call and count **distinct
ProcessIds**, not concurrent ones — the deviation's signature was many
short-lived processes, not many simultaneous ones.

**Server side (cluster).** Count new `sshd` sessions attributable to the run:

```bash
journalctl --user-unit ssh -S "<window start>" | grep -c "Accepted "
# or, without journal access:
last -F -n 200 "$USER" | awk '$0 ~ /still logged in|<date>/'
```

**Acceptance.**

| Mode | Bring-up | Any number of later operations |
| --- | --- | --- |
| `ssh_forward` | exactly 1 new `sshd` session | 0 new |
| `brokered_tcp` / `udp_rendezvous` | 0 (after deployment) | 0 new; passes with SSH unavailable entirely |

The reference deviation, for comparison: on 1.5.10 a single `jarvis_describe`
call opened **9** fresh SSH connections and about 35 seconds of pure handshake,
and the cluster recorded 9 matching new `sshd` sessions.

The unit tests assert the same invariant in-process by counting calls to the
injected channel-process factory — one call is one SSH connection — in
`tests/test_owned_session_channel.py`.

## What is legitimately still SSH

Deployment and lifecycle, not the runtime control plane:

- cluster bootstrap (`bootstrap.py`) and endpoint-service install/restart
  (`deployment.py`);
- `session start` / `session teardown` / cleanup finalize and report read, which
  must work when the owned API is not running and therefore cannot ride a
  channel that depends on it;
- `session start-watch`, which observes a start transition that by definition
  precedes the owned API. It is no longer a redial loop: the desktop sends its
  remaining deadline as `--wait-seconds` and the cluster-local command blocks
  against durable state, so one watch is one connection regardless of how long
  the start takes.
