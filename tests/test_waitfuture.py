# Parman extends Python concurrent.futures to facilitate parallel workflows.
# Copyright (C) 2023 Toon Verstraelen
#
# This file is part of Parman.
#
# Parman is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# Parman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Unit tests for parman.waitfuture."""

import random
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from time import sleep

import pytest
from parman.waitfuture import WaitGraph


def func(x, t):
    sleep(t)
    return 2 * x


def error_func(x, t):
    sleep(t)
    raise ValueError


def digest_tuple(*args):
    return tuple(args)


def test_minimal(pool):
    wait_graph = WaitGraph()
    f = pool.submit(func, 1.0, 0.1)
    wf = wait_graph.submit([f])
    assert wf.result(timeout=1) is None
    assert wf.done()
    assert f.done()
    assert f.result(timeout=1) == 2.0


def test_two_after(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.1)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    assert wf.result(timeout=1) == (2.0, 4.0)
    assert wf.done()
    assert f1.done()
    assert f2.done()
    assert f1.result(timeout=1) == 2.0
    assert f2.result(timeout=1) == 4.0


def test_zero():
    wait_graph = WaitGraph()
    wf = wait_graph.submit([])
    assert wf.done()


def test_two_after_done(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.1)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    assert f1.result(timeout=1) == 2.0
    assert f2.result(timeout=1) == 4.0
    assert f1.done()
    assert f2.done()
    assert wf.result(timeout=1) == (2.0, 4.0)
    assert wf.done()


def test_as_completed(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.2)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    results = set()
    for f in as_completed([f1, f2, wf], timeout=1):
        results.add(f.result())
    assert results == {2.0, 4.0, (2.0, 4.0)}


def test_wait_all(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.2)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    wait([f1, f2, wf], timeout=1)
    assert f1.done()
    assert f2.done()
    assert wf.done()


def test_wait_any(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(func, 2.0, 0.2)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    not_done = {f1, f2, wf}
    while not_done:
        not_done = wait(not_done, timeout=1, return_when=FIRST_COMPLETED)[1]
    assert f1.done()
    assert f2.done()
    assert wf.done()


def test_cancel(pool1):
    wait_graph = WaitGraph()
    f1 = pool1.submit(func, 1.0, 0.1)
    f2 = pool1.submit(func, 2.0, 5.0)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    f2.cancel()
    assert wf.result(timeout=1) == (2.0, None)
    assert wf.done()
    assert f1.done()
    assert f2.cancelled()


def test_exception(pool):
    wait_graph = WaitGraph()
    f1 = pool.submit(func, 1.0, 0.1)
    f2 = pool.submit(error_func, 2.0, 0.2)
    wf = wait_graph.submit([f1, f2], digest_tuple)
    with pytest.raises(ValueError):
        wf.result()
    assert wf.done()
    assert f1.done()
    assert f2.done()
    assert f1.exception(timeout=1) is None
    assert isinstance(f2.exception(timeout=1), ValueError)
    assert isinstance(wf.exception(timeout=1), ValueError)


def test_after_after(pool):
    wait_graph = WaitGraph()
    f = pool.submit(func, 1.0, 0.1)
    af1 = wait_graph.submit([f], digest_tuple)
    af2 = wait_graph.submit([af1], digest_tuple)
    assert af2.result(timeout=1) == ((2.0,),)
    assert af2.done()
    assert af1.done()
    assert af1.result(timeout=1) == (2.0,)
    assert f.done()
    assert f.result(timeout=1) == 2.0


@pytest.mark.parametrize("seed", range(3))
@pytest.mark.parametrize("max_workers", [15, 45, 150])
@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, ProcessPoolExecutor])
@pytest.mark.parametrize("size", [10, 30, 100])
@pytest.mark.parametrize("end", ["normal", "reverse", "wait", "as_completed"])
def test_larger(seed, max_workers, executor_class, size, end):
    """Test designed to trigger potential race conditions."""

    def digest(*args):
        return hash(args)

    random.seed(seed)
    wait_graph = WaitGraph()
    with executor_class(max_workers) as pool:
        futures = [pool.submit(func, i, random.uniform(0.001, 0.010)) for i in range(size)]
        expected = [2 * i for i in range(size)]
        for _i in range(size):
            step = random.randrange(1, 10)
            offset = random.randrange(size)
            futures.append(wait_graph.submit(futures[offset::step], digest))
            expected.append(digest(*expected[offset::step]))
        pairs = list(zip(futures, expected, strict=True))
        if end == "reverse":
            pairs = pairs[::-1]
        elif end == "wait":
            wait(futures, timeout=5)
        elif end == "as_completed":
            list(as_completed(futures, timeout=5))
        else:
            assert end == "normal"
        for future, result in pairs:
            assert future.result(timeout=5) == result
