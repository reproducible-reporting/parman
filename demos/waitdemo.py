#!/usr/bin/env python3
"""A simple usage example of parman.waitfuture.

Note: Direct use of WaitGraph by end-users is expected to be marginal.
It is used extensively by Parman Runners to hide technical complexities.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from parman.waitfuture import WaitGraph


def func(x, t):
    """Wait t seconds and return x."""
    time.sleep(t)
    return x


def digest_tuple(*args):
    """Simple function to be passed on to the executor. Returns args as tuple."""
    return tuple(args)


wait_graph = WaitGraph()
with ThreadPoolExecutor() as pool:
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.2)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    print("f1 =", f1)
    print("f2 =", f2)
    print("wf =", wf)
    print()
    # We use as_completed to wait for `f1` and `wf`, and print their results.
    # The future `wf` will wait for `f2` to finish.
    for f in as_completed([f1, wf]):
        print("Completed:", f, "result =", f.result())
    print()
    print("f1 =", f1)
    print("f2 =", f2)
    print("wf =", wf)
