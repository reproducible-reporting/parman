"""API and resource definitions."""

from pathlib import Path


def mock(pause: float, config: Path):
    return Path("__computed__")


resources = {"parsl_executors": "all"}
