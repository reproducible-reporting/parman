# Development notes

## How to make a release

- Mark release in `CHANGELOG.md`
- Make a new commit and tag it with `vX.Y.Z`
- Trigger the PyPI GitHub Action: `git push origin main --tags`

## Development setup

```bash
git clone git@github.com:reproducible-reporting/parman.git
cd parman
pre-commit install
python -m venv venv
echo 'source venv/bin/activate' > .envrc
direnv allow
pip install -U pip
pip install -e .[dev]
hash -r
pytest -vv
```
