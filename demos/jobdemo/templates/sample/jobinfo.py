"""API and resource defintions."""

from pathlib import Path


def mock(pause: float, models: list[Path], temperature: int, sample_size: int):
    return [Path(f"__boot_sample_{i}__") for i in range(sample_size)]


resources = {"parsl_executors": "all"}
