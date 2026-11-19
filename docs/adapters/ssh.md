# The ssh adapter

The `ssh` adapter reaches a site with the `ssh` and `scp` already on your
machine. It needs no daemon, no agent and no open port on the site: every call
is one `ssh` or `scp` run, so your keys, your `~/.ssh/config`, your jump hosts
and your ssh agent all apply.

It serves `describe`, `run` (with `status`, `cancel` and `find` on the jobs it
starts), `exec`, `files` and `serve`.

## Adding a site

A site entry names a host from your ssh configuration:

```yaml
- id: ares
  name: Ares
  adapter: ssh
  config:
    ssh_profile: ares
  policy: open
  spend: {ask: never}
```

Check that `ssh ares hostname` works in a terminal first. If it asks you
anything, see [sites that ask for a code](#sites-that-ask-for-a-code).

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `ssh_profile` | (required) | A `Host` from `~/.ssh/config`, or `user@host` |
| `ssh_config` | `~/.ssh/config` | Another ssh_config file to use |
| `ssh_options` | none | Extra `Key=Value` options, passed with `-o` |
| `remote_dir` | `.clio-relay` | Relay's directory on the site, under your home unless absolute |
| `scheduler` | `auto` | `auto`, `none`, `slurm`, `pbs`, `lsf` or `cobalt` |
| `interactive_sign_in` | `false` | The site asks for a code on every connection |
| `askpass` | relay's own | The program that answers ssh's prompts |
| `ssh_command`, `scp_command` | `ssh`, `scp` | The programs to run |
| `connect_timeout_s` | 30 | ssh's `ConnectTimeout` |
| `command_timeout_s` | 300 | When a `describe`, `status` or `cancel` call counts as stalled. The next poll tries again |
| `retries` | 5 | How many times relay repeats a call or copy whose connection failed or dropped, waiting 2, 4, 8, 16, then 30 seconds. exec, and a write that may not overwrite, are repeated only when the connection never opened |

Jobs and transfers have no time limit. A long upload or a job that runs for
days is allowed to finish.

`exec` has no time limit either unless the call gives one. While a command
prints nothing, the caller hears that it is still running. When the call ends,
times out or is cancelled, or the connection drops, the command and everything
it started are stopped on the site.

`exec` runs its argument list from your home directory, with no shell in
between, so `$HOME`, `*` or `;` in an argument reach the program as they are.
A program that is only defined by your login shell, such as `module` from
Lmod, runs in a login shell (`bash -l`), which gets the same argument list
unchanged. On a guarded site, `exec` runs only the read-only list; anything
else is refused with `policy_refused`.

Relay asks the scheduler for no walltime of its own. A job's walltime is its
`duration`, then the walltime its package's contract gives for the run's
parameters, then the site's `defaults.duration`. With none of these, the batch
script has no time line, and the scheduler's default for the partition or
queue applies.

The spend gate's charge is in node-hours: nodes times walltime. A job with no
walltime has an unknown cost, so its charge has no amount and no node-hours,
and the gate asks for it unless the site's `spend.ask` is `never`.

## How a job runs

1. Relay renders the batch script on your machine with
   [psij-python](https://exaworks.org/psij-python/). The scheduler is the one
   the site has, found by `describe` when `scheduler` is `auto`.
2. Inputs are staged by content into `<remote_dir>/content/cas/` with `scp`.
   An input already on the site is not sent again. If the job deletes an input
   from its directory, for example a file of secrets it reads once, relay
   removes the site's stored copy too when the job ends or is cancelled, unless
   another job still uses it. An input marked `transient` goes when the job
   ends in any case, both the job's copy and the stored one, even if the job
   was cancelled before it started.
3. One `ssh` call writes the script into the job's directory and submits it.
   The directory is `<remote_dir>/jobs/<operation id>/` and holds the script,
   the submit result, `stdout`, `stderr` and the exit code. On Slurm, a job's
   `stdin_path`, `stdout_path` and `stderr_path` may use sbatch's filename
   patterns (`%j`, `%x`, `%u`, `%N`, `%A`, `%a` and `%%`), so
   `hostname-%j.out` becomes `hostname-27874.out`.
4. `status` asks the scheduler through psij's own status command, and reads
   the exit code from the job's directory once the job ends.

Relay keeps no connection open between calls, so it can restart at any time.
After a restart, `find` reads the job's directory and returns the same job, so
a retried `run` never submits a second copy.

`cancel` uses the scheduler's own command (`scancel`, `qdel`). Your command
gets `SIGTERM` first, so a `trap` in it can clean up, and is killed only after
the scheduler's grace period (Slurm's `KillWait`, PBS's `kill_delay`). Some
Slurm sites signal only the batch shell; relay's Slurm script passes the
signal on to your command and waits for it to finish its trap, so it behaves
the same on every site. A command over several lines, such as a multi-line
`bash -c` script, is handled the same way.

On a shared filesystem such as NFS, a file written on one node can stay
invisible on another for a minute or two. Relay lists a directory before it
checks a file in it, which makes the filesystem look again, so job records,
exit codes and outputs show up as soon as they are written.

Schedulers forget finished jobs; Slurm drops them from `squeue` after
`MinJobAge`, often five minutes. When the scheduler no longer lists a job,
`status` reads the exit code the job wrote into its directory. With no exit
code, it asks Slurm accounting (`sacct`) if the site keeps it. With neither,
the job stays open with `native_state` `unknown` and a message saying so,
rather than being called failed. A later poll can still find an exit code, and
`cancel` ends it.

### Hosts with no scheduler

With `scheduler: none`, or on a host where `auto` finds none, the job runs
detached in its own session (`setsid`, or `nohup` where the host has no
`setsid`). It keeps running after ssh disconnects, and a later `status` reads
its exit code from the job directory. `cancel` stops the whole process group.

## Sites that ask for a code

Some sites ask for a one-time code or a password on every connection. Relay
never types into ssh itself.

- **Without `interactive_sign_in`**, relay runs ssh with `BatchMode=yes`. A
  site that asks is refused with `interactive_auth_required`, and the refusal
  names the `worker` adapter, which holds one signed-in connection for you.
- **With `interactive_sign_in: true`**, relay signs in once. The site shows
  as needing sign-in; you press "Start sign-in" on relay's page and answer
  each prompt there. That starts an ssh master connection
  (`ControlMaster`), and every later call reuses it with `BatchMode=yes`, so
  you are asked once, not on every call. When the master closes, the site
  asks you to sign in again. The master closes when relay stops, so none is
  left running after it, and signing in again while one is up reuses it.

`askpass` replaces the program that shows the prompts, for example with a
program of your own that reads codes from a token. With such a program, relay
signs in by itself and only asks you if the program fails.

## Files

`files` follows [the files contract](../files.md). A relative path is under
`remote_dir`, `~/` is your home on the site, and an absolute path is the
site's own. Each operation is one `ssh` call, and the bytes travel on its
standard input and output.

A chunk goes into a temporary file first and joins `<path>.relay-part` only
once all of it has arrived, so a link that drops partway adds nothing. A chunk
sent again after its answer was lost is recognised by the part's size and the
hash of its last bytes, and taken without change; so is the last chunk sent
again after the part was moved into place. `delete` never follows a symlink and
refuses the work directory, your home and `/`, even through a symlink to them.

On Ares, from the login node, a 4 MiB chunk takes about 2 s through the
adapter, of which 0.9 s is opening the ssh connection, and a 4 MiB read about
0.8 s. Carrying a chunk through relay's MCP server adds 0.1 to 0.3 s on an
idle machine; a loaded login node can make that several times longer.

## Serving a port

`serve` forwards a port from the site with `ssh -L`, adding `-J` through the
login node when the service runs on a compute node.

## When the connection drops

Every call is its own ssh run, so a dropped link costs only the call it hit.
Relay repeats that call up to `retries` times, waiting 2, 4, 8, 16 and then
30 seconds, and logs each attempt as "retry n of N". The repeat is safe
because each step checks before it acts: a copy skips content already on the
site, a submit first looks for the job it may already have submitted, and
`status` and `cancel` only read or stop. A job keeps running on the site while
the link is down.

Three calls are repeated only when the connection never opened, because the
command may already have run: `exec`, a whole `files` write that may not
overwrite, and `files` delete. Such a call fails with `unreachable` and the
caller decides. Chunks of a `files` write are always repeated.

A failed login, an unknown or changed host key and a missing key are not
repeated: trying again would give the same answer.

## Troubleshooting

| Refusal | What it means |
| --- | --- |
| `unreachable` | ssh could not connect or log in. The message says why and what to do, and `data.cause` names it (below) |
| `timeout` | A `describe`, `status` or `cancel` call stalled. The next poll tries again |
| `interactive_auth_required` | The site asks for a code. Turn on `interactive_sign_in`, or use the `worker` adapter |
| `needs_sign_in` | The signed-in connection closed. Sign in again on relay's page |
| `adapter_unavailable` | `ssh` or `scp` is not installed on this machine |

| `cause` | What to do |
| --- | --- |
| `host_key_unknown` | Connect once with `ssh <host>`, check the fingerprint and accept it. Relay never accepts a host key it cannot check |
| `host_key_changed` | The host's key differs from the one in `known_hosts`. Ask the site whether it changed; if it did, remove the old line with `ssh-keygen -R <host>` |
| `key_refused` | The site took no key relay offered. Add the key to your agent (`ssh-add`) or name it with `IdentityFile`; the message names key files it could not read |
| `unknown_host` | The host name does not resolve. Check the name and your VPN |
| `network` | The connection failed or dropped after the retries. Check the network and VPN |
| `ssh` | Any other ssh failure. The message carries ssh's last line |
