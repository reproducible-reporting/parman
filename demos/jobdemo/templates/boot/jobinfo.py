"""API and resource definitions."""

from pathlib import Path


def mock(pause: float, boot_size: int):
    """Return a mocked result, used to define the return type."""
    return [Path(f"__boot_config_{i}__") for i in range(boot_size)]


resources = {"parsl_executors": "all"}
