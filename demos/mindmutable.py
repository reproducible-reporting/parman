#!/usr/bin/env python
"""Simple example showing the dangers of mutable Future arguments.

When submitting a function to an executor with mutable arguments,
these can be changed prior to the actual execution.
The result seen by the remote function depends on the executor,
which is generally not what you want.

The Parman runners make a deepcopy of closure arguments, to avoid this point of confusion.
The (remotely) executed closure always sees the arguments as they were at the time of submission.
"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from time import sleep


def func(x):
    """Log x, wait, log x and return x."""
    print(f"  Start func, x = {x}")
    sleep(0.3)
    print(f"  End func, x = {x}")
    return x


print("ThreadPoolExecutor")
with ThreadPoolExecutor(max_workers=1) as pool:
    l1 = [1]
    future = pool.submit(func, l1)
    sleep(0.1)
    l1[0] = 2  # This will affect func.


print("ProcessPoolExecutor")
with ProcessPoolExecutor(max_workers=1) as pool:
    l2 = [1]
    future = pool.submit(func, l2)
    sleep(0.1)
    l2[0] = 2  # This will not affect func.
