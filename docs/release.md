# Release process

A release is a tag. Pushing a `v<version>` tag runs `release.yml`, which
checks that the tag matches the package version, builds the wheel and source
distribution once, and publishes them to PyPI through trusted publishing and
the `pypi` environment. Nothing else gates publication.

`ci.yml` runs on pull requests, on `main` and on tags. It runs ruff, pyright
and pytest, and builds the wheel to check its entry point. Do not wait on it
to cut a release; check it after, and fix any failure in the next patch.

## Version

Update the version in:

- `pyproject.toml`
- `src/clio_relay/__init__.py`

`scripts/bump_release_version.py` changes every pinned site in one pass, and
`scripts/check_release_identity.py` checks that they agree.

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

If you want a GitHub Release page, create it from the same tag afterwards and
attach the wheel and source distribution from the `dist` artifact of the
release run.

## After publication

Install the published wheel in a clean environment and check the entry point:

```powershell
uv run --no-project --with "clio-relay==$(uv version --short)" clio-relay --help
```
