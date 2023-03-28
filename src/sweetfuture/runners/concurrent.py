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

from ..clerks.base import ClerkBase
from ..recursive import recursive_get, recursive_transform
from .base import RunnerBase
from .jobinfo import validate

__all__ = ("ConcurrentRunner", "validating_wrapper", "FutureResult")


@attrs.define
class ConcurrentRunner(RunnerBase):
    """Run jobs asynchronously with an Executor"""

    clerk: ClerkBase = attrs.field()
    executor = attrs.field(default=None)

    def __attrs_post_init__(self):
        if self.executor is None:
            self.executor = ProcessPoolExecutor()

    def call(self, func, kwargs, kwargs_api, result_api, resources):
        kwargs = recursive_transform(
            lambda _, field: field.result() if isinstance(field, FutureResult) else field,
            kwargs,
        )
        validate("kwarg", kwargs, kwargs_api)
        future = self.executor.submit(validating_wrapper, func, kwargs, result_api)
        return recursive_transform(
            lambda mulidx, field: FutureResult(
                future,
                mulidx,
            ),
            result_api,
        )

    def wait(self):
        """Wait until all jobs have completed."""
        self.executor.shutdown()


def validating_wrapper(func, kwargs, result_api):
    result = func(**kwargs)
    validate("result", result, result_api)
    return result


@attrs.define
class FutureResult:
    future = attrs.field()
    mulidx: tuple = attrs.field()

    def result(self):
        return recursive_get(self.future.result(), self.mulidx)
