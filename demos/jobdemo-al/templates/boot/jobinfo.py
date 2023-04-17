"""API and resource defintions."""

from pathlib import Path


def get_result_mock(pause: float, boot_size: int):
    return [Path(f"__boot_config_{i}__") for i in range(boot_size)]


resources = {"parsl_executors": "all"}