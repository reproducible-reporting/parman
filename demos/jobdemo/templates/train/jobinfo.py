"""API and resource definitions."""

from pathlib import Path


def mock(pause: float, examples: list[Path], seed: int):
    """Return a mocked result, used to define the return type."""
    return Path("__model__")


resources = {"parsl_executors": "all"}
