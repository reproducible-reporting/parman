#!/usr/bin/env python3
"""A simple usage example of parman.scheduler.

Note: Parman Runner classes hide most of the technical complexity seen in this example.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from parman.scheduler import Scheduler


def func(x, t):
    """Wait t seconds and return x."""
    time.sleep(t)
    return x


def add(x1, x2, t):
    """Wait t seconds and return x1 + x2."""
    time.sleep(t)
    return x1 + x2


with ThreadPoolExecutor() as pool:

    def user_submit(user_function, futures, t):
        """Function called by the scheduler to submit a user_function to a pool."""
        args = [future.result() for future in futures]
        args.append(t)
        return pool.submit(user_function, *args)

    with Scheduler(user_submit) as scheduler:
        f1 = pool.submit(func, 1.0, 0.1)
        f2 = pool.submit(func, 2.0, 0.2)
        dependencies = [f1, f2]
        sf = scheduler.submit([add, dependencies, 0.3], {}, dependencies)
        print("f1 =", f1)
        print("f2 =", f2)
        print("sf =", sf)
        print()
        # We use as_completed to wait for `f1` and `sf`, and print their results.
        # The future `sf` will wait for `f2` to finish and for the scheduled future to finish.
        for f in as_completed([f1, sf]):
            print("Completed:", f, "result =", f.result())
        print()
        print("f1 =", f1)
        print("f2 =", f2)
        print("sf =", sf)
