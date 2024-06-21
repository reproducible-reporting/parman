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
"""Concurrent job runner, wrapper around a standard Executor from concurrent.futures."""

from concurrent.futures import Future, ThreadPoolExecutor

import attrs

from ..closure import Closure
from .future import FutureRunnerBase

__all__ = ("ConcurrentRunner",)


@attrs.define
class ConcurrentRunner(FutureRunnerBase):
    """Run jobs asynchronously with an Executor"""

    executor = attrs.field(default=None)

    def __attrs_post_init__(self):
        FutureRunnerBase.__attrs_post_init__(self)
        if self.executor is None:
            self.executor = ThreadPoolExecutor()

    def _submit(self, closure: Closure) -> Future:
        """Submit a closure to the executor."""
        closure = self._unpack_data(closure)
        print(f"Submitting {closure.describe()}")
        with self._submit_lock:
            return self.executor.submit(Closure.validated_call, closure)

    def shutdown(self):
        """Wait for all futures to complete."""
        FutureRunnerBase.shutdown(self)
        self.executor.shutdown()
