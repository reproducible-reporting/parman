from pathlib import Path


def get_result_mock(
    initial: Path,
    cutoff: float = 2.5,
    temperature: float = 300.0,
    nsteps: int = 10000,
    stride: int = 100,
):
    return Path("__final__")
