# Release

`clio-relay` uses `uv`, hatchling, and GitHub Actions.

## Local Checks

Run these before cutting a release:

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
uv build
```

## Bootstrap Source

`cluster bootstrap` supports two deployment sources:

- From a repository checkout, it deploys the committed git `HEAD`. The checkout must be clean so the remote cluster receives the same tree that was reviewed and committed.
- From an installed wheel or PyPI package, it deploys the packaged JARVIS assets and installs `clio-relay==<version>` on the remote cluster.

Before publishing, verify that the wheel contains `clio_relay/assets/jarvis-packages/clio_relay/`. That packaged asset path is what makes bootstrap work without a local checkout.

## Version

Update both version declarations:

- `pyproject.toml`: `[project].version`
- `src/clio_relay/__init__.py`: `__version__`

Use a conventional commit for the version change, for example:

```text
chore: release 0.9.0
```

## GitHub Release

Create a tag that matches the package version:

```powershell
git tag v0.9.0
git push origin v0.9.0
```

Then create a GitHub release for that tag. The release workflow builds the source distribution and wheel, verifies the metadata, uploads the files as release artifacts, and publishes to PyPI when trusted publishing is configured.

## PyPI Publishing

Publishing is wired for GitHub trusted publishing from `iowarp/clio-relay`. Configure PyPI with:

- project name: `clio-relay`
- owner: `iowarp`
- repository: `clio-relay`
- workflow file: `release.yml`
- environment: `pypi`

The workflow uses OpenID Connect and does not need a PyPI token secret. Keep the `pypi` GitHub environment protected so release publishing remains intentional.
