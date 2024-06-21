"""API and resource definitions."""

from pathlib import Path


def mock(pause: float, config: Path):
    """Return a mocked result, used to define the return type."""
    return Path("__computed__")


resources = {"parsl_executors": "all"}
