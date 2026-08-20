## clio-relay 1.6.7

This release makes relay errors consistent and machine-readable across every
surface, lets a caller wait for a specific pattern in a running job's output
instead of polling, makes a job's own produced files fetchable as artifacts,
fixes several bugs that could leave a cluster bootstrap stuck or unable to
redeploy, and continues an ongoing internal reliability effort to keep the
codebase in small, focused modules.

### Consistent, typed error responses

Every error the relay returns — over MCP tools, the HTTP API, or the browser
gateway — now carries the same typed envelope (`clio-relay.error.v1`) and a
stable set of reason codes, instead of each surface inventing its own error
shape. This makes failures easier to detect and handle programmatically
without parsing free-text messages. (#231)

### Wait for a pattern instead of polling

`relay_observe` gained `until_pattern`: give it a regular expression and it
returns as soon as that pattern appears in the job's stdout, stderr,
progress, or events — with the matching excerpt, its position, and a
timestamp — or as soon as the job finishes, whichever comes first. This
replaces polling with a single call that blocks until something worth
seeing happens.

### Job output files are fetchable artifacts

Files a JARVIS execution produces (logs, outputs, frames) are now
registered as artifacts on the job that produced them, each carrying its
name, size, and SHA-256, with lineage recording which run produced it.
Fetching one reuses the existing bounded artifact-read path, so no bytes
are copied at registration time. (#252)

### Reading your own job's output

Fixed a gap where a completed job's produced bytes had no route back to
the caller under a user profile: the artifact-listing and artifact-reading
tools are now advertised to user-profile callers, so a finished job's
output can actually be listed and read, not just referenced. (#256)

### More reliable cluster (re-)bootstrap

Fixed several bugs that could leave a cluster bootstrap stuck or unable to
redeploy onto a host that already had clio-relay on it:

- An interrupted bootstrap that crossed a certain point without finishing
  could get permanently wedged; recovery is now state-aware and completes
  the interrupted step instead. (#247)
- Redeploying onto a host that already had a JARVIS environment installed
  previously failed outright; the relay now builds the replacement
  environment alongside the old one and swaps it in atomically. (#254)
- That same atomic-swap fix now also covers a full reinstall over a host
  whose entire relay installation already exists, not just its JARVIS
  environment. (#257)

### Dev mode warns instead of silently blocking

Under dev mode, identity and version gates (contract version, artifact
identity, and similar checks) now warn loudly and proceed instead of
quietly withholding a result — the checks still run and still record
what they found, they just no longer stop the request. (#242)

### JARVIS MCP contract v3.7.1

The bundled JARVIS MCP user contract advances to `v3.7.1`.

### Internal

Ongoing internal work continued reorganizing large modules (job-queue
handling, error routing, cluster-bootstrap logic) into smaller, focused
files for maintainability. This is not expected to change any external
behavior.
