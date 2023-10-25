#!/usr/bin/env python

from pathlib import Path

from parman.scripts.sbatch_wait import cached_run


def main():
    cache_time, out = cached_run(["/bin/bash", "./slowscript.sh"], Path("cache.txt"), 100)
    print("cache_time", cache_time)
    print(out)


if __name__ == "__main__":
    main()
