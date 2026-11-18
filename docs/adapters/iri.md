# The iri adapter

The `iri` adapter uses the IRI Facility API, the common REST interface that
NERSC, ALCF, OLCF and ESnet serve for compute and files. Its job description
follows PSI/J, so a relay job maps onto it field by field. Relay needs only the
facility's base URL and a token; nothing is installed on the site.

It serves `describe`, `run` (with `status`, `cancel` and `find`) and `files`.
The adapter uses `httpx`, which relay already has, so the `iri` extra installs
nothing more.

## Adding a site

```yaml
- id: perlmutter-iri
  name: NERSC Perlmutter through IRI
  adapter: iri
  identity: globus
  config:
    base_url: https://api.iri.nersc.gov
    api_version: v2
    resource_id: <compute resource id>
    scope: https://auth.globus.org/scopes/ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api
  defaults: {account: <your project>}
```

Find `resource_id` in the facility's `GET /api/v2/compute/resources` (or
`/api/v1/...`) list. `describe` prints that list when you are signed in. If
files live on a different resource, set `filesystem_resource_id`.

## Signing in

The API takes a bearer token. Where the facility accepts Globus tokens, as
NERSC does, the adapter asks relay's `globus` identity for one with the
facility's scope. Where a facility issues its own tokens, set `token_command`
to a command that prints one, for example the facility's own CLI. The command
has no time limit; relay logs while it is quiet. With no token
the site shows as needing sign-in and calls are refused with `needs_sign_in`.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `base_url` | (required) | The facility's IRI API, such as `https://api.iri.nersc.gov` |
| `api_version` | `v2` | `v1` or `v2` |
| `resource_id` | (required) | The compute resource jobs go to |
| `filesystem_resource_id` | `resource_id` | The resource `files` uses |
| `identity` | `globus` | Relay identity that holds the token |
| `resource_server`, `scope` | none | The facility's Globus scope, for sign-in |
| `token_command` | none | A command that prints an access token instead |
| `root` | none | Where relative paths start; needed for relative paths |
| `job_root` | `.clio-relay/jobs` | Where job directories go, under `root` |
| `content_dir` | `.clio-relay/content` | Where staged inputs go |
| `poll_s` | 1 | First wait between polls of a file operation |
| `stall_s` | 600 | How long a file operation may stay unchanged before relay stops waiting |

## How a job runs

1. Relay writes the job's inputs into `<job_root>/<operation id>/` through the
   files API.
2. It submits the job with `POST /compute/job/<resource_id>`. On v2 the request
   carries an `Idempotency-Key` made from the operation id, so a retried `run`
   returns the same job instead of submitting a second one. v1 has no such key;
   relay keeps its own record of the job id instead.
3. `status` reads the job; `cancel` sends `DELETE` for it. `stdout` and
   `stderr` default to the job directory.

## Files

Each file call is a facility task that relay polls until it ends; there is no
time limit while the task is changing. The API moves files of up to 5 MiB
through `read` and `write`. For anything larger, use the facility's Globus
collection with the `globus` adapter. Facilities return downloads in different
shapes; the adapter reads plain text and the base64 forms.

## Versions

Relay speaks v1 and v2 of the API (OpenAPI 1.0.0 and 2.0.0). They differ in
where the parameters of file calls go and in the idempotency key; `api_version`
picks one.
