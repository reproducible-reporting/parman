"""API and resource defintions."""

from pathlib import Path


def get_result_mock(examples: list[Path], seed: int):
    return Path("__model__")


resources = {
    "parsl_executors": "all",
    "dask_submit_kwargs": {},
}
