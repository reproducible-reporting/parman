"""API and resource defintions."""

from pathlib import Path


def get_kwargs_api():
    return {"examples": list[Path], "seed": int}


def get_result_api(kwargs):
    return Path


resources = {
    "parsel_executors": "all",
    "dask_submit_kwargs": {},
}
