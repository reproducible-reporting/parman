"""API and resource defintions."""

from pathlib import Path


def mock(pause: float, examples: list[Path], seed: int):
    return Path("__model__")


resources = {"parsl_executors": "all"}
