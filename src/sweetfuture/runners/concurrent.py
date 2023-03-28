# SweetFuture enables transparent parallelization.
# Copyright (C) 2023 Toon Verstraelen
#
# This file is part of SweetFuture.
#
# SweetFuture is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# SweetFuture is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Concurrent job runner, wrapper around a standard Executor from concurrent.futures."""

from concurrent.futures import ProcessPoolExecutor

import attrs

from ..closure import Closure
from .future import FutureRunnerBase

__all__ = ("ConcurrentRunner",)


@attrs.define
class ConcurrentRunner(FutureRunnerBase):
    """Run jobs asynchronously with an Executor"""

    executor = attrs.field(default=None)

    def __attrs_post_init__(self):
        if self.executor is None:
            self.executor = ProcessPoolExecutor()

    def _submit(self, closure: Closure):
        return self.executor.submit(Closure.validated_call, closure)

    def wait(self):
        """Wait until all jobs have completed."""
        self.executor.shutdown()
