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
"""Unit tests for parman.runners."""

from concurrent.futures import ThreadPoolExecutor
from time import sleep

from parman.closure import Closure
from parman.metafunc import MetaFuncBase
from parman.runners.concurrent import ConcurrentRunner


class KeepSleeping:
    def __init__(self):
        self.asleep = True

    def __call__(self) -> int:
        while self.asleep:
            sleep(0.1)
        return 5


class MockMetaFunc(MetaFuncBase):
    def __init__(self):
        self.too_early = True

    def __call__(self, arg: int) -> int:
        if self.too_early:
            raise AssertionError("Called MetaFunc too early.")
        return 2 * arg

    def get_result_mock(self, arg: int) -> int:
        return 0


def test_result_wait_args():
    keep_sleeping = KeepSleeping()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(keep_sleeping)
        runner = ConcurrentRunner(schedule=True, executor=executor)
        try:
            # Create a future that will take indefinitely.
            metafunc = MockMetaFunc()
            closure = Closure(metafunc, [future])
            outcome = runner(closure)
        finally:
            metafunc.too_early = False
            keep_sleeping.asleep = False
            runner.shutdown()
        assert outcome.result() == 10
