# Release

`clio-relay` uses `uv`, hatchling, and a staged GitHub Actions release. A tag
creates an inert candidate payload in an unprivileged job. Protected-main code
normalizes and attests that payload, maintainer-sealed operator live evidence
gates publication to PyPI, released-artifact evidence then exercises the
published persistent `uv tool` path, and only the final evidence-verifying workflow publishes
the GitHub release.

## Local checks

Run the same checks as the release builder before tagging:

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
uv run clio-relay release validate-local --project-root .
```

`release validate-local` writes stable JSON even when a check fails. Skipped
tests fail the gate. It exports the exact `uv.lock` dependency set with hashes,
audits that set with the lock-installed `pip-audit`, builds exactly one wheel
and source distribution with the exact lock-installed Hatchling backend,
validates both with the lock-installed Twine, and installs the exact wheel into
a clean environment containing only hash-checked production dependencies from
`uv.lock`. It also builds the exact source distribution back into a wheel and
installs and launches that result in a second clean, hash-locked runtime
environment. This executable sdist smoke runs only in the unprivileged local,
CI, and tag-build gate; no write-, OIDC-, environment-, attestation-, or
promotion-capable job executes candidate distribution code. Its report records
the distribution paths, sizes, SHA-256 digests,
backend identity, commands, resolved freeze, and bounded outputs.
Protected promotion jobs instead inflate the gzip source distribution into a
private regular temporary file under a hard uncompressed-tar byte ceiling,
then parse the bounded tar and wheel topology without invoking either archive.

The 1.0 policy has an explicit `release_blockers` list. The evaluator fails
closed even when all acceptance reports pass while any declared blocker
remains. The reviewed 1.0 candidate has no declared implementation blockers:
the exact JARVIS-CD and clio-kit releases are pinned, while containment,
retention, cross-platform CI, and live Gray-Scott proof are enforced by named
checks and target requirements. Missing evidence still fails the gate. Add a
new blocker immediately if review discovers work that cannot be represented by
an existing acceptance requirement, and remove it only after implementation
and evidence are present on the reviewed release commit.

## Bootstrap source

`cluster bootstrap` supports two deployment sources:

- A repository checkout deploys the committed `HEAD`. The checkout must be
  clean so the remote cluster receives the reviewed tree.
- An installed wheel deploys packaged JARVIS assets and installs that exact
  wheel on the remote cluster.

Before tagging, verify the wheel contains
`clio_relay/assets/jarvis-packages/clio_relay/`. Bootstrap uses that packaged
asset path and must not depend on a local checkout.

## Version and tag

Update both version declarations:

- `pyproject.toml`: `[project].version`
- `src/clio_relay/__init__.py`: `__version__`

Commit the release change with a conventional commit, then create and push an
exact matching tag:

```powershell
$Tag = "v1.0.0"
git fetch origin main
$ReviewedMainSha = (git rev-parse refs/remotes/origin/main).Trim()
if ((git rev-parse HEAD).Trim() -ne $ReviewedMainSha) {
  throw "release commit is not the exact reviewed origin/main commit"
}
git tag $Tag $ReviewedMainSha
git push origin $Tag
```

From the tag push until `finalize-release.yml` succeeds, keep `main` frozen at
`$ReviewedMainSha`. Every privileged stage fetches live `origin/main` again and
requires it, the protected workflow checkout, and the release tag to identify
that exact commit. If `main` advances before PyPI publication, abandon the
candidate and cut a new version from the newly reviewed commit; never move or
replace the protected tag. Do not merge unrelated work during this window. In
particular, advancing `main` after PyPI publication would prevent the immutable
evidence chain from being finalized, because published package versions cannot
be replaced.

All six same-tag release workflows use the shared
`clio-relay-release-<tag>` concurrency group with cancellation disabled. This
serializes tag build, staging, both evidence seals, PyPI promotion, and final
publication. It does not replace the freeze: every privileged stage still
fetches live `origin/main` and fails unless main, the reviewed SHA, the tag, and
the protected workflow checkout are identical.

The tag-push workflow rejects a tag that does not match the package version,
checked-out commit, and freshly fetched `origin/main` commit. It runs the
complete local gate, builds exactly one wheel and one source distribution, and
creates `SHA256SUMS`, but has zero `GITHUB_TOKEN` permissions. It fetches the
public tag without repository credentials and can upload only an Actions
artifact. It cannot mint an OIDC identity, attest evidence, or create a release.

After that read-only workflow succeeds, stage the payload by dispatching only
from protected `main`:

```powershell
gh workflow run stage-candidate.yml --ref main -f tag=$Tag `
  -f reviewed_main_sha=$ReviewedMainSha
```

The `live-validation` environment admits the staging job only from protected
`main`. The job selects the sole successful tag-push run for the exact tag and
commit. Before download, it binds the sole nonexpired artifact to the exact run
id, attempt, head SHA, name, byte count, and API SHA-256 digest; it downloads by
artifact id, verifies the archive digest, rejects unsafe or extra members, and
extracts bounded inert files. Protected-main code then builds current CI and
repository-governance receipts, replaces the tag manifest with a canonical
manifest, attests the bytes, and creates the draft with an explicit target of
the reviewed commit. Tag-supplied workflow code never receives release-write or
OIDC authority.

An existing draft is reusable only when every staged asset is byte-for-byte
identical and its complete asset-name set equals the six staged candidate
assets. The workflow never replaces a candidate distribution.

Enable GitHub immutable releases for the repository before the 1.0 release.
The default workflow token cannot read the repository administration endpoint,
so configure an `IMMUTABLE_RELEASES_READ_TOKEN` secret on the protected
`release-finalization` environment. Use a fine-grained token restricted to this
repository with repository Administration read permission. Finalization calls
`GET /repos/{owner}/{repo}/immutable-releases`, requires `enabled: true`, and
records the deterministic result in `RELEASE-CLAIMS.json`. It repeats the same
admin-read check immediately before changing the draft to public and refuses
publication if the endpoint is unavailable, disabled, or has changed.

## Live validation of the candidate

The cluster labels named in `release-gate-1.0.yaml` are the concrete evidence
instances selected for this release, not an allowlist in the product. Any
operator can add another physical target through the cluster registry and can
make it release-blocking by adding requirements for that label to the policy.
Adding a target or changing the evidence matrix requires configuration and
policy updates only, with no target-name branch in relay code.

Download the draft wheel and manifest, verify both the digest and the signed
tag-build provenance, and compute the digest locally:

```powershell
$Tag = "v1.0.0"
New-Item -ItemType Directory -Force .clio-relay\candidate | Out-Null
gh release download $Tag --pattern "*.whl" --pattern "SHA256SUMS" `
  --dir .clio-relay\candidate
$Wheel = (Get-ChildItem .clio-relay\candidate\*.whl).FullName
$Expected = ((Get-FileHash -Algorithm SHA256 $Wheel).Hash).ToLowerInvariant()
$WheelName = Split-Path $Wheel -Leaf
$ManifestLine = Get-Content .clio-relay\candidate\SHA256SUMS |
  Where-Object { $_ -match "[ *]$([Regex]::Escape($WheelName))$" }
if ($ManifestLine -notmatch "^$Expected [ *]") { throw "candidate digest mismatch" }
$Commit = gh api "repos/iowarp/clio-relay/commits/$Tag" --jq .sha
gh attestation verify $Wheel `
  --repo iowarp/clio-relay `
  --signer-workflow iowarp/clio-relay/.github/workflows/stage-candidate.yml `
  --source-ref refs/heads/main `
  --source-digest $Commit `
  --deny-self-hosted-runners
```

Every acceptance command for the evidence instances currently named Ares and
homelab must execute through that local wheel,
not a checkout or an already published PyPI package. Record its independently
computed digest in each report:

```powershell
$env:CLIO_RELAY_VALIDATION_LAUNCHER = "uv-tool"
$env:CLIO_RELAY_VALIDATION_ARTIFACT_SHA256 = $Expected
$Source = "wheel:$([Uri]::new($Wheel).AbsoluteUri)"
$env:UV = (Get-Command uv).Source
uv tool install --force --python 3.12 --no-config $Wheel
$Relay = (Get-Command clio-relay).Source
$env:CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE = $Relay

& $Relay cluster bootstrap `
  --cluster ares `
  --relay-wheel $Wheel `
  --validation-launcher uv-tool `
  --validation-install-source $Source `
  --report .clio-relay\validation-reports\validation-ares-bootstrap.json

& $Relay live-test `
  --cluster ares `
  --validation-install-source $Source `
  --jarvis-yaml .\pipeline.yaml `
  --report .clio-relay\validation-reports\validation-ares-live-test.json

& $Relay jarvis-mcp-validate `
  --cluster ares `
  --arguments-json-file .\jarvis-run.json `
  --validation-launcher uv-tool `
  --validation-install-source $Source `
  --report .clio-relay\validation-reports\validation-ares-jarvis-mcp.json
```

Run every scenario required by the candidate-mode derivation of
[`release-gate-1.0.yaml`](release-gate-1.0.yaml), including Ares bootstrap,
JARVIS and non-JARVIS MCP, existing LAMMPS Spack discovery/location plus one
separate absent-to-fresh-install-to-location transition, explicit SLURM
phases, a bounded 20-step JARVIS-native Gray-Scott progress and artifact query,
safe cleanup and explicit owned-job cancellation, homelab transport and
cleanup, and the gateway runtime.
Use the executable [1.0 live acceptance runbook](release-acceptance-1.0.md) and
its tracked 17-report matrix. The matrix carries a canonical semantic SHA-256,
the candidate and released filename prefixes, and the ordered logical report
identities. Candidate and released preflight reject any missing, extra, renamed,
or reordered logical entry; the policy evaluator then requires every one of the
17 report documents to participate in a satisfied requirement. Both seals,
both decisions, and final claims bind the same matrix digest and order. The
runbook assigns fresh candidate or released pipeline, session, gateway,
invocation, report, and output identities and keeps the two evidence stages
disjoint.
Upload each unique machine-readable report to the draft without replacing an
earlier report:

```powershell
gh release upload $Tag .clio-relay\validation-reports\validation-*.json
```

An uploaded JSON file is not promotion evidence by itself. These reports are
generated by an operator-run validation process; they do not contain a TPM,
target-held signing key, or other independent proof that the target executed
the recorded commands. After every required report is present, dispatch the
attestation workflow from protected `main`, passing the candidate tag as verified
data:

```powershell
gh workflow run live-validation-attest.yml --ref main -f tag=$Tag
```

The repository's current GitHub plan does not support environment required-
reviewer rules. A write-capable repository maintainer must manually dispatch
sealing from protected `main`; the workflow resolves that actor to a positive
GitHub identity and requires `write`, `maintain`, or `admin` permission. The
maintainer may also be the source author, report producer, or report uploader.
The seal is therefore maintainer authorization and integrity binding, not an
independent review claim. All privileged environments admit only protected-
branch dispatches and disable administrator bypass. This is a trust boundary,
not defense in depth: a tag dispatch would let the tag supply the code that
validates itself. The workflow requires protected `main`, the tag commit, and
the checked-out source to be identical, then checks every non-local report's
passing status, exact tag and commit, clean source identity, detected wheel
install source, persistent uv-tool launcher and RECORD closure, distribution
version, and candidate wheel digest.
It then runs the candidate-mode gate with protected-main code and locked dependencies;
the candidate wheel remains inert in every write-, OIDC-, and attestation-
capable job. The operator-produced reports, not execution inside the privileged
job, prove that the exact wheel ran on each selected target. The workflow signs
every live JSON report and a deterministic
`LIVE-VALIDATION-BINDING.json` that lists the exact report filenames, report
ids, scenarios, clusters, and SHA-256 digests. Re-uploaded, added, or modified
reports invalidate promotion unless the authorized workflow seals the exact new
set. The binding labels this trust boundary as
`maintainer_sealed_operator_evidence`, records the sealer's numeric GitHub
identity and write-capable permission, and explicitly records
`producer_execution_verified=false`; neither the GitHub attestation nor the
release notes may be described as independent target-produced proof.

The release policy also requires every non-local report to contain exactly one
evidenced, verified physical `cluster_target`. The gate validates its live
hostname, SSH host key, scheduler provider, and optional scheduler/site markers
against the operator pins. It records a canonical target-identity SHA-256 for
each cluster label and fails if any report reuses a label for a different
physical identity. These labels are evidence keys for this release, not a
runtime target allowlist. Operators can register arbitrary cluster targets, and
a later release can add any of them to `policy.targets` and `requirements`
without changing relay code.

The 1.0 worker and JARVIS requirements also verify two receipt-bound native
capabilities: JARVIS-CD must expose its execution handle, record, progress, and
query APIs in its execution interpreter, and the released clio-kit wheel must
expose the locked JARVIS MCP contract with those exact schemas. Legacy
`clio_relay.package_progress_adapters` entry points remain compatibility
diagnostics, but they are neither required nor accepted as native release
evidence.

These are immutable-candidate reports, not released-artifact reports. Their
`released_artifact` field remains false because PyPI publication has not yet
occurred. They can authorize publication of the exact bytes to PyPI, but they
cannot authorize a 1.0 claim or publication of the GitHub release.

Diagnostic checkout runs and wheels from any other build may find defects, but
they cannot satisfy the release gate.

## Publish the candidate to PyPI

Run the `publish validated candidate to PyPI` workflow with the draft tag:

```powershell
gh workflow run release-gate.yml --ref main -f tag=$Tag `
  -f reviewed_main_sha=$ReviewedMainSha
```

The workflow:

1. resolves the tag to its commit and requires it to equal both the explicitly
   supplied reviewed main SHA and a freshly fetched `origin/main`;
2. requires the complete draft asset-name set to contain only the authorized
   candidate inputs (plus exact idempotent promotion-record names on recovery),
   then downloads exactly one wheel, one source distribution, the local report,
   every live validation report, and the authorized binding;
3. independently checks `SHA256SUMS`; parses every wheel and source-distribution
   member with strict path, type, count, per-file, compressed, and uncompressed
   aggregate bounds; verifies both core-metadata identities; and verifies the
   GitHub attestation identity for protected-main
   `.github/workflows/stage-candidate.yml` and the reviewed commit;
4. verifies the binding and every live report were attested by the protected-
   `main` `.github/workflows/live-validation-attest.yml` at that same commit, then
   rejects missing, extra, modified, or differently bound reports;
5. derives a candidate-mode policy from the final policy and runs the
   protected-main gate over reports bound to the independently computed
   candidate digest, without importing or executing either downloaded
   distribution;
6. publishes only the verified wheel and source distribution to PyPI through
   the OIDC-scoped `pypi` environment;
7. verifies PyPI reports the exact candidate filenames and digests; and
8. attests and attaches `PYPI-PROMOTION.json` while requiring the GitHub
   release to remain a draft.

Promotion is recovery-safe after a partial run. If the version is absent from
PyPI, the workflow uploads both distributions. If PyPI already has an exact
subset of the candidate files, the workflow preserves those bytes and uploads
only the missing distribution. If the complete filename-to-SHA-256 map already
matches, it skips the upload and continues. Any additional filename or digest
mismatch fails closed. `skip-existing` is enabled only after that preflight so
an interrupted two-file upload is rerunnable; a mandatory postflight then
requires the complete PyPI map to equal the candidate exactly. The workflow
does not rebuild distributions, accept a caller-supplied digest, or publish the
GitHub release. A rerun after successful PyPI publication verifies the existing
bytes, regenerates the same publication record, and continues without upload.
If a prior run already attached either recovery asset, the workflow verifies
its GitHub attestation and requires the candidate decision to be byte-identical
to a fresh gate result. An existing promotion record must also be
byte-identical to a record regenerated from the current PyPI filename, URL, and
digest map. These checks run in the gate and again immediately before the PyPI
state decision.

## Live validation of the released artifact

After `PYPI-PROMOTION.json` is attached, rerun every required Ares and homelab
scenario through the actual index-resolved package. Do not pass a wheel path or
override the install source. Preserve the independently verified published
wheel digest in each report:

```powershell
$Version = $Tag.TrimStart("v")
New-Item -ItemType Directory -Force .clio-relay\published | Out-Null
gh release download $Tag --pattern "PYPI-PROMOTION.json" `
  --dir .clio-relay\published
$Promotion = Get-Content .clio-relay\published\PYPI-PROMOTION.json | ConvertFrom-Json
$env:CLIO_RELAY_VALIDATION_LAUNCHER = "uv-tool"
$env:CLIO_RELAY_VALIDATION_ARTIFACT_SHA256 = $Promotion.wheel_sha256
$env:UV = (Get-Command uv).Source
Remove-Item Env:UV_INDEX, Env:UV_EXTRA_INDEX_URL, Env:UV_INDEX_URL `
  -ErrorAction SilentlyContinue

uv tool install --force --refresh --no-config `
  --default-index https://pypi.org/simple `
  --python 3.12 "clio-relay==$Version"
$Relay = (Get-Command clio-relay).Source
$env:CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE = $Relay

& $Relay cluster bootstrap --cluster ares `
  --validation-launcher uv-tool `
  --validation-install-source "pypi:clio-relay==$Version" `
  --report .clio-relay\validation-reports\released-validation-ares-bootstrap.json

& $Relay live-test --cluster ares --jarvis-yaml .\pipeline.yaml `
  --report .clio-relay\validation-reports\released-validation-ares-live-test.json

& $Relay jarvis-mcp-validate `
  --cluster ares `
  --arguments-json-file .\jarvis-run.json `
  --validation-launcher uv-tool `
  --report .clio-relay\validation-reports\released-validation-ares-jarvis-mcp.json
```

The report must detect `pypi` itself as both its effective and detected install
source, record launcher `uv-tool`, verify its persistent environment and
installed RECORD closure, set `released_artifact` true, and match the published
wheel digest. Renaming a candidate report does not satisfy this contract.
Upload the distinct released reports without replacing assets, then
seal them from protected `main`:

```powershell
gh release upload $Tag `
  .clio-relay\validation-reports\released-validation-*.json
gh workflow run released-validation-attest.yml --ref main -f tag=$Tag
```

Released-evidence sealing applies the same write-capable maintainer dispatch
rule and permits that maintainer to seal evidence they produced or uploaded.
The workflow verifies
the signed PyPI promotion record, current PyPI filenames and
digests, every released report's exact source identity, and the final published
policy. It evaluates that policy with protected-main code and locked
dependencies; it does not execute either the draft wheel or the PyPI package in
the privileged sealing job. Instead, every accepted report must prove that its
operator installed `clio-relay==<version>` once with `uv tool install` against
the public PyPI index, reused that persistent executable, and bound the exact
published digest. The workflow signs every released
report, `RELEASED-VALIDATION-BINDING.json`, and
`released-release-gate-1.0.json`.

## Finalize the GitHub release

Only after released evidence is sealed, dispatch:

```powershell
gh workflow run finalize-release.yml --ref main -f tag=$Tag `
  -f reviewed_main_sha=$ReviewedMainSha
```

The finalization environment admits only protected branches and cannot be
bypassed by administrators. The workflow
reverifies the full build, candidate, PyPI, and released-evidence
attestation chain; requires the final policy decision to pass with no remaining
blockers; confirms the current PyPI map still equals the candidate bytes; and
creates `RELEASE-CLAIMS.json`. Immediately after live mutation-authority
revalidation and before publication it requires the complete GitHub release
asset set to equal the attested distributions, reports, bindings, decisions,
promotion record, manifest, and claims file by exact asset id, name, size, and
SHA-256 digest, with no additional payloads. Both reads request a 100-record
first page and an explicit second page; the configured 96-asset ceiling and an
empty second page are recorded in the inventory, so pagination cannot silently
truncate the comparison. After publication it requires the
immutable inventory to remain byte-for-byte and id-for-id identical before the
workflow can succeed. The claims file separates the local quality gate from
only those live requirements and reports selected through the released-artifact
path. The workflow attaches and attests
the claim set before making the GitHub release public. A retry after publication
succeeds only when the claim asset and generated release notes are unchanged and
the claim digest has an authorized `finalize-release.yml` attestation from the
exact protected-main commit.

## PyPI trusted publishing

Configure PyPI trusted publishing for:

- project: `clio-relay`
- owner: `iowarp`
- repository: `clio-relay`
- workflow: `release-gate.yml`
- environment: `pypi`

The publication uses OpenID Connect and needs no PyPI token. Every privileged
environment must set `protected_branches=true`,
`custom_branch_policies=false`, and `can_admins_bypass=false`; a `v*` environment
policy is insufficient because an unreviewed tag can contain a modified workflow
at the trusted publisher's configured path. If the repository plan supports
environment required-reviewer rules, enable them for `pypi` and
`release-finalization` as defense in depth. The release chain does not claim
those optional rules are active.

Repository governance is proved from GitHub's effective rules for `main`, not
from the legacy branch-protection administration endpoint. The effective rule
set must enforce strict results from the exact seven GitHub Actions CI jobs, at
least one approving review, stale-review dismissal, last-push approval,
conversation resolution, no force pushes, and no deletion. Active tag rules
must prevent `v*` update and deletion. Detailed ruleset responses must report
that the current workflow token can never bypass each contributing ruleset.
GitHub may omit the global `bypass_actors` list for the workflow token; receipts
record whether that list was visible and never convert an omitted list into a
false empty-list claim. Global actor policy therefore remains an administrator
audit, while the automated decision proves the current token's non-bypass
status.

Do not cut a new artifact for the same version after a partial publication;
rerun the workflow so its preflight can verify and complete the exact file set.
Protect release tags so only reviewed release commits can invoke staging and
attestation workflows.
