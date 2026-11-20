# The worker adapter

The `worker` adapter runs jobs on a cluster you reach over ssh, through a small
relay worker it places on the cluster. After one ssh sign-in, every call goes
over an outbound link the worker holds to a rendezvous server (frps). Relay
does not keep an ssh session open, and the cluster needs no inbound port.

It serves `describe`, `run` (with `status`, `cancel` and `find`), `exec`,
`files` and `serve`.

## Adding a site

```yaml
- id: ares
  name: Ares
  adapter: worker
  config:
    ssh_profile: ares
    scheduler: slurm
    rendezvous:
      address: frps.example.org
      port: 443
      protocol: wss
      token_env: RELAY_FRP_TOKEN
  policy: guarded
```

`ssh_profile` is a `Host` in your `~/.ssh/config`. Put the user name, keys,
jump hosts and anything else ssh needs there. Relay passes only the profile
name to ssh.

## Signing in

While no worker link is up, the site shows as needing sign-in. You start the
sign-in from relay's page. Relay runs ssh once, and each prompt ssh shows, such
as a password or a one-time code, appears on that page as an ask. Nothing you
type reaches relay's logs.

In that one ssh pass relay:

1. installs `uv` on the cluster if it is missing;
2. installs `clio-relay` into the worker directory with `uv tool install`;
3. copies frpc over, unless the site has its own (`site_frpc`);
4. writes the worker's settings and starts it in the background;
5. waits for the worker to report ready, then closes ssh.

The worker then dials out to the rendezvous and keeps that link up. Relay dials
the same rendezvous and reaches the worker through it. Each sign-in places a
fresh worker generation with new secrets and stops the previous one.

When the link drops, calls are refused with `unreachable`. If the worker is
gone, sign in again.

## The link

`link_mode` picks how relay reaches the worker:

| `link_mode` | frp type | How calls travel |
| --- | --- | --- |
| `brokered_tcp` (default) | `stcp` | Through the rendezvous server. Works wherever both ends reach frps |
| `udp_rendezvous` | `xtcp` | Straight to the cluster, once a UDP hole punch through the rendezvous succeeds |

Set it in the site's config, or pick it on the Compute page, where the site's
form shows it as a choice. The worker's proxy follows the mode, so changing it
asks for a sign-in, which places the worker again.

In `udp_rendezvous` a punch that fails is refused with `punch_failed`. Relay
does not switch to `brokered_tcp` on its own. Retrying tries the same punch.
To go through the rendezvous, set `link_mode: brokered_tcp`.

Services that `serve` starts get their own tunnels; the rendezvous `direct`
setting in [running a rendezvous host](../rendezvous-host.md#point-relay-at-it)
picks those.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `ssh_profile` | required | The ssh config `Host` that signs in to the cluster |
| `ssh_config_file` | `~/.ssh/config` | Another ssh config file |
| `scheduler` | `auto` | `slurm`, `pbs`, `lsf`, `cobalt`, `none`, or `auto` to pick the one whose submit command is on the cluster's `PATH` |
| `work_dir` | in the worker directory | Where job directories go on the cluster |
| `rendezvous` | relay's `CLIO_RELAY_FRPS_ADDR` and `CLIO_RELAY_FRP_TOKEN` | The frps server, as below |
| `remote_dir` | `~/.local/share/clio-relay/worker/<site>` | The worker's directory on the cluster |
| `relay_wheel` | none | A local clio-relay wheel to install instead of the PyPI release |
| `relay_requirement` | `clio-relay==<this version>` | What `uv` installs on the cluster |
| `site_frpc` | upload relay's frpc | An frpc already on the cluster |
| `python` | `3.12` | The Python `uv` installs the worker with |
| `submit_timeout_s` | 60 | How long one scheduler submit command may take |
| `link_mode` | `brokered_tcp` | `brokered_tcp` or `udp_rendezvous`; see [the link](#the-link) |

`rendezvous` takes:

| Setting | What it does |
| --- | --- |
| `address`, `port` | frps as relay reaches it |
| `protocol` | `wss` (default), `websocket` or `tcp` |
| `site_address`, `site_port` | frps as the cluster reaches it, when that differs |
| `token_env` or `token_file` | Where relay reads the frps token |
| `stun_server` | The STUN server both ends ask in `udp_rendezvous`; `stun.nextcloud.com:3478` by default. It must support RFC 5780 |

## How a job runs

The worker runs the `local` adapter on the cluster, so a job there behaves as
it would on a `local` site with the same scheduler. Relay sends each call over
the link, and the worker answers with the same results or refusals.

- `run` stages the job's inputs on the cluster by digest first, from relay's
  content store, so only missing inputs travel.
- `status`, `cancel` and `find` read the job on the cluster. A handle stays
  valid while the worker directory does, across relay and worker restarts.
- `exec` runs a command on the login node, with the same output limit and stall
  reports as `local`.
- `files` reads and writes under the worker's directory, in chunks.
- `serve` starts a service job and carries its traffic over the same link.

## Upgrades

A worker that an older relay placed is not used. Calls to its site are refused
with `needs_sign_in`, and the next sign-in stops it and places a current worker
in the same directory. Jobs and staged content there stay.

## On the cluster

The worker has no commands to run by hand. Placement starts it as
`clio-relay worker site-serve`. [Operations](../operations.md) lists what it
keeps in its directory and how to check, stop or remove it.

It runs on the login node, so it stays light:

- It runs at the lowest CPU and I/O priority (`nice -n 19`, `ionice -c3`).
- Where the site's systemd user manager lingers, it runs in a
  `systemd-run --user --scope` with `CPUQuota=25%` and `MemoryMax=512M`.
  Elsewhere it runs under `ulimit -v` (2 GiB of address space) and `ulimit -t`
  (7200 CPU seconds a process). `login_limits` changes these.
- One runs per user and site. It holds a lock on `run/site.lock`, and a second
  one refuses.
- It exits after `idle_exit_s` (1800 s by default, 0 for never) with no request
  from relay, and writes `run/stopped.json` with the reason. The site's next
  call asks for a sign-in, which places it again. Jobs keep running meanwhile:
  JARVIS submitted them, not the worker.

What the worker runs for a call, such as `jarvis ppl submit` or `spack spec`,
runs under the same limits. A site whose Spack concretization needs more
memory raises `login_limits.memory_mb`.

`scheduler: auto` refuses to run jobs on the login node when it finds no
scheduler but the host shows a batch system, as on
[`ssh`](ssh.md#hosts-with-no-scheduler).
