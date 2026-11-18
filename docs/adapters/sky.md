# The sky adapter

The `sky` adapter runs jobs with [SkyPilot](https://skypilot.readthedocs.io/)
managed jobs, on the clouds, Kubernetes and Slurm that SkyPilot supports. It
also gives the spend gate SkyPilot's price for a job before it runs.

It serves `describe` and `run` (with `status`, `cancel` and `find`).

## Installing SkyPilot

SkyPilot pins versions of libraries that relay also uses, so it does not go in
relay's own environment. Relay never imports it. Install it in an environment
of its own and relay runs it there as a separate process:

```
uv venv ~/.clio-relay/sky
uv pip install --python ~/.clio-relay/sky 'skypilot[aws]==0.14.3'
```

Pick the clouds you use in the brackets, such as `skypilot[aws,gcp]`. Relay
looks for SkyPilot's Python in this order:

1. the site's `python` setting;
2. `~/.clio-relay/sky/bin/python`;
3. the Python next to a `sky` command on your `PATH`, or the one its first line
   names.

When none is found, building the site is refused with `adapter_unavailable` and
these install steps. `describe` reports the Python it uses and SkyPilot's
version.

## Adding a site

```yaml
- id: aws
  name: AWS through SkyPilot
  adapter: sky
  config:
    infra: aws
    cpus: 2+
    credentials:
      provider: aws
      env:
        AWS_ACCESS_KEY_ID: aws-access-key-id
        AWS_SECRET_ACCESS_KEY: aws-secret-access-key
  policy: guarded
  spend: {ask: always}
```

SkyPilot also reads the cloud profiles already on your machine, such as
`~/.aws/credentials`. With `credentials`, relay instead reads the keys from your
keyring, under the service `clio-relay`, and hands them to SkyPilot as those
variables. A missing key shows the site as needing sign-in, and you add it on
relay's page.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `python` | found as above | SkyPilot's Python |
| `infra` | SkyPilot's choice | Where to run, such as `aws`, `gcp/us-central1` or `k8s` |
| `instance_type`, `cpus`, `memory`, `accelerators` | none | What each node needs, in SkyPilot's terms |
| `use_spot` | `false` | Use spot instances |
| `disk_size`, `region`, `zone` | none | Passed to SkyPilot |
| `setup` | none | Commands run once on each node before the job |
| `workdir` | none | A local directory SkyPilot copies to the job |
| `env` | none | Variables for SkyPilot itself |
| `credentials` | none | Keyring entries to hand to SkyPilot, as above |
| `stall_s` | 1800 | How long SkyPilot may print nothing before relay stops waiting for it |

A job can override any resource setting with a `sky.` custom attribute, such as
`sky.accelerators: A100:8`.

## Price

`price` asks SkyPilot's catalog what the job's resources cost for the job's
walltime, times its node count, and returns it in US dollars. It works offline,
with no cloud account. For example, two `m6i.large` nodes on AWS for one hour
come to 0.192 USD on demand. The spend gate shows this price before the job
starts.

## How a job runs

1. Relay renders the job as one shell command. Inputs are copied into a work
   directory that SkyPilot syncs to the job.
2. It launches a SkyPilot managed job named `relay-<operation id>`. The
   managed job brings up the instances, runs the command and tears the
   instances down when it ends.
3. `status` reads SkyPilot's job queue. When the job ends, relay downloads its
   log as the job's output.
4. `cancel` cancels the managed job.

SkyPilot reports no exit code for a managed job. A failed job is recorded with
exit code 1 unless SkyPilot's failure reason names one.

There is no time limit on a SkyPilot call. Relay only stops waiting when
SkyPilot prints nothing for `stall_s`.
