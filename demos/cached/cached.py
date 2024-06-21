#!/usr/bin/env python
"""Demo of the cached_run function of the parman-sbatch-wait script."""

from pathlib import Path

from parman.scripts.sbatch_wait import cached_run


def main():
    """Main program."""
    cache_time, out = cached_run(["/bin/bash", "./slowscript.sh"], Path("cache.txt"), 100)
    print("cache_time", cache_time)
    print(out)


if __name__ == "__main__":
    main()
