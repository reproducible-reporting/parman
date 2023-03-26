from pathlib import Path


def get_kwargs_api():
    return {"boot_size": int}


def get_result_api(kwargs):
    return [Path] * kwargs["boot_size"]


resources = {
    "parsel_executors": "all",
    "dask_submit_kwargs": {},
}
