"""API and resource definitions."""

from pathlib import Path


def mock(pause: float, models: list[Path], temperature: int, sample_size: int):
    """Return a mocked result, used to define the return type."""
    return [Path(f"__boot_sample_{i}__") for i in range(sample_size)]


resources = {"parsl_executors": "all"}
