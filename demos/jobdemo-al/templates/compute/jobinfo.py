"""API and resource defintions."""

from pathlib import Path


def get_result_mock(pause: float, config: Path):
    return Path("__computed__")


resources = {"parsl_executors": "all"}