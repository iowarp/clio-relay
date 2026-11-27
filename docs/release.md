# Release process

A release is a tag. Pushing a `v<version>` tag runs `release.yml`, which
checks that the tag matches the package version, builds the wheel and source
distribution once, and records their `SHA256SUMS`. It attaches all three to
a GitHub Release for the tag, and publishes them to PyPI through trusted
publishing and the `pypi` environment. A pre-release version (PEP 440, such
as `2.0.0a1` or `2.0.0rc1`) goes to PyPI too, where installers skip it unless
asked for it by version or with `--prerelease allow`; its GitHub Release is
marked a pre-release and is never the latest. Nothing else gates publication.

`ci.yml` runs on pull requests, on `main` and on tags. It runs ruff, pyright
(held to the error count in `scripts/check_pyright_ratchet.py`), the file
size and nested class checks, and pytest, and builds the wheel to check its
entry point. Do not wait on it to cut a release; check it after, and fix any
failure in the next patch.

## Version

Set the same version in both places:

- `version` in `pyproject.toml`
- `__version__` in `src/clio_relay/__init__.py`

## Tag and publish

Merge the change, then tag the merged commit on `main` and push the tag:

```powershell
git switch main
git pull --ff-only origin main
if (git status --porcelain) { throw "release checkout is dirty" }
$Tag = "v$(uv version --short)"
git tag $Tag
git push origin $Tag
```

`release.yml` builds with `CLIO_RELAY_RELEASE_BUILD=1`, so the build hook
requires a commit, a clean tree and the exact `v<project.version>` tag. An
untagged, differently tagged or dirty tree fails the build. Ordinary builds
outside a release do not set it and still work.

A pre-release can be tagged from a branch other than `main`. A plugin that
needs a fixed build can depend on the release's wheel by URL; its lock records
the wheel's hash, which matches the release's `SHA256SUMS`.

## Publish an earlier release

A tag cut before its version could go to PyPI can still be published, with the
exact files of its GitHub Release. Run `release.yml` by hand from a `v*` tag
ref (the `pypi` environment admits only those and `main`), naming the earlier
tag; the job checks the files against the release's `SHA256SUMS` first:

```powershell
gh workflow run release.yml --ref v2.0.0a37 -f tag=v2.0.0a36
```

## After publication

Install the published wheel in a clean environment and check the entry point:

```powershell
uv run --no-project --with "clio-relay==$(uv version --short)" clio-relay --help
```
