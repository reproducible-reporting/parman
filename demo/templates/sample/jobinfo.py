"""API and resource defintions."""

from pathlib import Path


def get_kwargs_api():
    return {"models": list[Path], "temperature": int, "sample_size": int}


def get_result_api(kwargs):
    return [Path] * kwargs["sample_size"]


resources = {
    "parsel_executors": "all",
    "dask_submit_kwargs": {},
}
