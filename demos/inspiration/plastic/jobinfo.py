from pathlib import Path


def mock(
    initial: Path,
    cutoff: float = 2.5,
    temperature: float = 300.0,
    nsteps: int = 10000,
    stride: int = 100,
):
    """Return a mocked result, used to define the return type."""
    return Path("__final__")
