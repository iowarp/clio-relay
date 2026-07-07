# release

`clio-relay` uses `uv`, hatchling, and GitHub Actions.

## local checks

Run these before cutting a release:

```powershell
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pytest
uv build
```

## version

Update both version declarations:

- `pyproject.toml`: `[project].version`
- `src/clio_relay/__init__.py`: `__version__`

Use a conventional commit for the version change, for example:

```text
chore: release 0.1.0
```

## github release

Create a tag that matches the package version:

```powershell
git tag v0.1.0
git push origin v0.1.0
```

Then create a GitHub release for that tag. The release workflow builds the source distribution and wheel, verifies the metadata, uploads the files as release artifacts, and publishes to PyPI when trusted publishing is configured.

## pypi publishing

Publishing is wired for GitHub trusted publishing. After the repository moves to the `iowarp` organization, configure PyPI with:

- project name: `clio-relay`
- owner: `iowarp`
- repository: `clio-relay`
- workflow file: `release.yml`
- environment: `pypi`

The workflow uses OpenID Connect and does not need a PyPI token secret. Keep the `pypi` GitHub environment protected so release publishing remains intentional.
