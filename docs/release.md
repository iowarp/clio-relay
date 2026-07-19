# Release process

`clio-relay` uses a release-first patch workflow. Publication is a maintainer
action, not the final step of a long GitHub Actions pipeline.

## Operating rule

For a bug fix, add a focused test that reproduces the failure, implement the
fix, and run only that focused test locally. Then push the branch, open and
merge the pull request, tag the merged commit, and publish the GitHub Release
immediately. Do not wait for pull-request, `main`, tag, or release workflows.

GitHub runs the full regression matrix on the tag. After the GitHub Release is
published, `release.yml` downloads its wheel and source distribution by numeric
release-asset ID, verifies `SHA256SUMS` and archive metadata, and sends those
exact attached bytes to PyPI through trusted publishing and the `pypi`
environment. These jobs are asynchronous. Check their state after publication,
but continue live testing while they run. If an asynchronous check fails,
reproduce that failure locally and include its fix with the next patch.

The older staged evidence workflows remain available for acceptance reporting.
They do not authorize or block immediate GitHub publication or the normal PyPI
path.

## Version

Update the release version in:

- `pyproject.toml`
- `src/clio_relay/__init__.py`
- `docs/release-gate-1.0.yaml`
- `examples/release-gate/report-matrix-1.0.json`
- the version-specific acceptance examples and assertions

After editing the matrix, recompute its canonical SHA-256 with the
`matrix_sha256` field omitted, then store that digest in both `matrix_sha256`
and the policy's `acceptance_matrix_sha256` field.

## Focused validation

Run the new test only. For a release-workflow change, for example:

```powershell
uv run pytest tests/test_release_workflows.py `
  -k "tag_workflow_validates_identity or published_release_uploads_exact"
```

Formatting or package construction required by the changed surface is not a
substitute for the focused behavioral test. Do not run the full regression
suite locally as part of the patch release loop.

## Merge and create the exact local tag

Push and merge the focused patch, then update the local `main` checkout and
create the package-version tag locally. The tag must exist before Hatch builds
the release artifacts because the build hook embeds the exact tag in both
distributions.

```powershell
git push -u origin HEAD
gh pr create --title "fix: ..." --body-file PR.md
gh pr merge --squash --delete-branch
git switch main
git pull --ff-only origin main
$Tag = "v1.4.5"
$Commit = (git rev-parse HEAD).Trim()
if (git status --porcelain) { throw "release checkout is dirty" }
if (git tag --list $Tag) { throw "release tag already exists locally: $Tag" }
git tag $Tag $Commit
```

Do not push the tag yet. Building before the local tag exists produces an
artifact without an exact source-tag identity.

## Build exact distributions

Build once from the merged release commit:

```powershell
Remove-Item -Recurse -Force dist -ErrorAction SilentlyContinue
$env:CLIO_RELAY_RELEASE_BUILD = "1"
try {
  uv build --out-dir dist
  if ($LASTEXITCODE -ne 0) { throw "release distribution build failed" }
} finally {
  Remove-Item Env:CLIO_RELAY_RELEASE_BUILD -ErrorAction SilentlyContinue
}
uv run twine check dist/*
$files = Get-ChildItem dist -File | Where-Object {
  $_.Name.EndsWith(".whl") -or $_.Name.EndsWith(".tar.gz")
}
if ($files.Count -ne 2) { throw "expected one wheel and one source distribution" }
$lines = foreach ($file in $files | Sort-Object Name) {
  $digest = (Get-FileHash -Algorithm SHA256 $file.FullName).Hash.ToLower()
  "$digest  $($file.Name)"
}
Set-Content -Encoding ascii -Path dist/SHA256SUMS -Value $lines
```

`CLIO_RELAY_RELEASE_BUILD=1` makes the build hook require a commit, a clean
tree, and the exact `v<project.version>` tag. An untagged, differently tagged,
or dirty source tree fails the build instead of creating a release artifact
with incomplete provenance. Ordinary untagged CI candidate builds remain
supported because they do not set release-build mode.

Do not rebuild between GitHub publication and PyPI publication. PyPI receives
the distributions downloaded from the published GitHub Release, not a second
CI build.

## Push the tag and publish

After the exact distributions have passed their local checks, push the tag and
publish those already-built bytes:

```powershell
git push origin $Tag
gh release create $Tag --draft --target $Commit --title "clio-relay 1.4.5" `
  dist/*.whl dist/*.tar.gz dist/SHA256SUMS --notes-file RELEASE.md
gh release edit $Tag --draft=false
```

Creating the draft with all assets first avoids a release-event race: the
`published` event is emitted only after the exact distributions and checksum
manifest are attached. Draft state is not a validation pause; publication
follows immediately in the same command sequence.

Repository rules may require a temporary maintainer bypass for an immediate
merge. If used, snapshot the complete ruleset, change only what is necessary,
merge, and restore the exact prior ruleset before tagging.

## Post-publication check

Check once; do not wait:

```powershell
gh run list --branch $Tag --limit 10
gh release view $Tag
```

The expected asynchronous work is:

- the tag identity receipt in `release.yml`;
- the full tagged regression matrix in `ci.yml`;
- the published-release trusted PyPI upload in `release.yml`.

Live released-artifact validation can begin as soon as the GitHub Release is
public. The acceptance procedure and machine-readable report schema remain in
[`release-acceptance-1.0.md`](release-acceptance-1.0.md) and
[`release-gate-1.0.yaml`](release-gate-1.0.yaml).
