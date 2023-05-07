# Development notes

## How to make a release

- Mark release in `CHANGELOG.md`
- Modify version in `pyproject.toml`
- Make a new commit and tag it with `vX.Y.Z`
- Trigger the PyPI GitHub Action: `git push origin main --tags`
