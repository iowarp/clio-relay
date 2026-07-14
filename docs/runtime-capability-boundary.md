# Runtime capability boundary

The relay treats JARVIS runtime metadata as a privileged control-plane channel, not as
application output. For scheduled and named launches, the worker precreates a private
sidecar with exclusive creation and mode `0600`, records its device, inode, owner, link
count, and mode, and sends the path, HMAC key, and filesystem anchor through a one-shot
broker pipe. The path and key are never present in the initial process environment or
command line. The wrapper starts Python with `-I -S`, consumes and closes both broker
file descriptors before importing JARVIS, and adds installed module roots as plain
`sys.path` entries. It does not process `.pth`, `sitecustomize`, or `usercustomize`.

Each runtime record has a strictly increasing sequence number and an HMAC over its
canonical JSON representation. The HMAC key is never serialized. The worker rejects
missing, replayed, reordered, or incorrectly signed records and rejects a sidecar whose
filesystem identity, ownership, hard-link count, or permissions changed. The same
anchor is stored in durable execution-cleanup state and enforced after worker restart.
If live authentication fails, the worker does not recover direct mode from the raw
record. It reconciles the durable scheduler intent instead. Unless direct mode was
already authenticated or exactly one provider-owned scheduler identity is proved, the
attempt remains unresolved, its cleanup marker stays pending, and all surviving
sidecars are retained for recovery or operator inspection.

The signer is a Linux-only release capability. Before importing JARVIS or package code,
it disables core dumps with `RLIMIT_CORE=0`, applies `PR_SET_DUMPABLE=0`, and verifies
the result with `PR_GET_DUMPABLE`. This blocks same-UID descendants from using ptrace,
`process_vm_readv`, `/proc/<pid>/mem`, or a core image to recover the HMAC key. Platforms
without that validated primitive fail before JARVIS load or scheduler submission.

This design deliberately trusts the JARVIS core and the JARVIS package code loaded in
that interpreter. They share an address space with the wrapper and therefore are part
of the runtime-metadata trusted computing base. Application commands, remote agents,
and MCP server descendants are not trusted with relay capabilities. The relay JARVIS
packages remove relay sidecar paths, tokens, secrets, and broker descriptors before
starting those descendants.

A named pipeline is not assumed to be scheduled. After `Pipeline.load()`, the wrapper
appends and fsyncs a signed direct-mode observation before calling `run()`. A separate
one-use proof is included only in the anchored sidecar; the worker durably stores its
hash before release, redacts the proof from normalized metadata, and removes the sidecar
during cleanup. This lets restart cleanup distinguish a direct named workload from an
unresolved scheduler submission without retaining the runtime HMAC key. A crash before
JARVIS establishes either mode remains unresolved by design.

The package-progress channel has a different producer boundary. Trusted relay JARVIS
packages receive `CLIO_RELAY_PROGRESS_FILE` and `CLIO_RELAY_PROGRESS_TOKEN` so they can
translate package-owned progress into relay records; they scrub both before launching
untrusted application descendants. The runtime-metadata HMAC key is not reused for
progress. Every progress record is an ordered canonical-JSON HMAC envelope; the token
is never included in the record or its metadata. Replays, gaps, reordered records,
modified payloads, and semantically invalid authenticated records fail the execution.
Operators should treat third-party JARVIS packages that directly consume the progress
environment as trusted package code, not as sandboxed applications.

For scheduler submission, the worker durably records a unique provider-native job-name
marker before releasing the broker. The wrapper receives that non-secret intent through
the private channel and appends an HMAC-signed copy before calling JARVIS. JARVIS also
atomically persists the marker and scripted submission state before invoking the
provider, then atomically persists the structured provider identity immediately after
parsing it. The relay-durable intent and exact provider query are the reconciliation
authority; JARVIS state is additional standalone recovery evidence.

If either the wrapper or worker dies across submission, the configured provider searches
both the active queue and a bounded accounting-history window using the exact marker,
scheduler user, and submission time. Ownership is accepted only for one match. Zero or
multiple matches remain explicitly unresolved and keep durable execution cleanup pending
for retry or operator intervention; zero active matches are never treated as proof that
submission did not occur. A reconciled job is recorded but is not canceled unless the
user explicitly requested scheduler-job cancellation.

For SLURM, active `squeue` and historical `sacct` results are always unioned and
deduplicated, even when the active queue already contains a match. Accounting history is
therefore required for exact reconciliation; an unavailable `sacct` fails closed. The
query and comparison windows include a five-second boundary tolerance for scheduler
timestamp precision without weakening the exact marker and user match.

The containment broker readiness signal is also capability-bound. The parent precreates
and pins a private regular file, retains its descriptor, and sends a random readiness
token only through the bounded setup pipe. It reads at most the token length plus one
byte from the pinned descriptor. A forged constant, oversized payload, replaced path,
changed identity, broker exit, or timeout cannot acknowledge release and always closes
the retained descriptor.
