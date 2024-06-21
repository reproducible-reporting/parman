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
"""Unit tests for parman.scheduler."""

import random
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from functools import partial
from time import sleep

import pytest
from parman.scheduler import Scheduler


def func(x, t):
    sleep(t)
    return 2 * x


def error_func(x, t):
    sleep(t)
    raise ValueError


def test_three(pool):
    with Scheduler(partial(pool.submit, func)) as scheduler:
        f1 = pool.submit(func, 1, 0.1)
        f2 = scheduler.submit([2, 0.1], {}, [f1])
        f3 = scheduler.submit([3, 0.1], {}, [f2])
        assert not f1.done()
        assert f1.running()
        assert not f2.done()
        assert not f2.running()
        assert not f3.done()
        assert not f3.running()
        assert f1.result() == 2
        assert not f2.done()
        assert not f2.running()  # Not correct due to a limitation in concurrent.futures.
        assert not f3.done()
        assert not f3.running()
        assert f2.result() == 4
        assert not f3.done()
        assert not f3.running()  # Not correct due to a limitation in concurrent.futures.
        assert f3.result() == 6


def test_zero(pool):
    with Scheduler(partial(pool.submit, func)) as scheduler:
        f = scheduler.submit([3, 0.1], {}, [])
        sleep(1.0)
        assert f.done()
        assert f.result() == 6


def test_exception1(pool):
    with Scheduler(pool.submit) as scheduler:
        f1 = pool.submit(error_func, 1, 0.1)
        f2 = scheduler.submit([func, 2, 0.1], {}, [f1])
        assert not f1.done()
        assert not f2.done()
        with pytest.raises(ValueError):
            f1.result()
        assert f1.done()
        assert isinstance(f1.exception(), ValueError)
        with pytest.raises(ValueError):
            f2.result()
        assert isinstance(f2.exception(), ValueError)
        assert f2.done()


def test_exception2(pool):
    with Scheduler(pool.submit) as scheduler:
        f1 = pool.submit(func, 1, 0.1)
        f2 = scheduler.submit([error_func, 2, 0.1], {}, [f1])
        assert not f1.done()
        assert not f2.done()
        assert f1.result() == 2
        assert not f2.done()
        with pytest.raises(ValueError):
            f2.result()
        assert f2.done()
        assert isinstance(f2.exception(), ValueError)


def test_cancel2(pool):
    with Scheduler(pool.submit) as scheduler:
        f1 = pool.submit(func, 1, 0.1)
        f2 = scheduler.submit([func, 2, 0.1], {}, [f1])
        assert not f1.done()
        assert not f2.done()
        f2.cancel()
        assert f2.done()
        assert f2.cancelled()
        assert f1.result() == 2


def test_cancel3(pool):
    with Scheduler(pool.submit) as scheduler:
        f1 = pool.submit(func, 1, 0.1)
        f2 = scheduler.submit([func, 2, 0.1], {}, [f1])
        f3 = scheduler.submit([func, 3, 0.1], {}, [f2])
        assert not f1.done()
        assert not f2.done()
        assert not f3.done()
        f2.cancel()
        assert f2.done()
        assert f2.cancelled()
        assert f1.result() == 2


@pytest.mark.parametrize("seed", range(3))
@pytest.mark.parametrize("max_workers", [15, 45, 150])
@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, ProcessPoolExecutor])
@pytest.mark.parametrize("size", [10, 30, 100])
@pytest.mark.parametrize("end", ["normal", "reverse", "wait", "as_completed"])
def test_larger(seed, max_workers, executor_class, size, end):
    """Test designed to trigger potential race conditions."""
    random.seed(seed)
    with executor_class(max_workers) as pool:
        futures = [pool.submit(func, i, random.uniform(0.001, 0.010)) for i in range(size)]
        expected = [2 * i for i in range(size)]

        def user_submit(dependencies, t):
            # Get results outside submit call...
            x = sum(dependency.result() for dependency in dependencies)
            return pool.submit(func, x, t)

        with Scheduler(user_submit) as scheduler:
            for _i in range(size):
                step = random.randrange(1, 10)
                offset = random.randrange(size)
                delay = random.uniform(0.001, 0.010)
                dependencies = futures[offset::step]
                futures.append(scheduler.submit([dependencies, delay], {}, dependencies))
                expected.append(2 * sum(expected[offset::step]))

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
