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
"""Abstract future job runner."""

from typing import Any

import attrs

from ..closure import Closure
from ..recursive import recursive_get, recursive_transform
from .base import RunnerBase

__all__ = ("FutureRunnerBase", "FutureResult")


@attrs.define
class FutureRunnerBase(RunnerBase):
    """Abstract base classes for running functions with Futures"""

    def __call__(self, closure: Closure) -> Any:
        wait_closure = Closure(
            closure.metafunc,
            FutureResult.wait_for_data(closure.args),
            FutureResult.wait_for_data(closure.kwargs),
        )
        future = self._submit(wait_closure)
        return FutureResult.promise_data(future, wait_closure.get_result_api())

    def _submit(self, closure: Closure):
        raise NotImplementedError


@attrs.define
class FutureResult:
    future = attrs.field()
    mulidx: tuple = attrs.field()

    @classmethod
    def wait_for_data(cls, data: Any) -> Any:
        return recursive_transform(
            lambda _, field: field.result() if isinstance(field, cls) else field,
            data,
        )

    @classmethod
    def promise_data(cls, future, data_api: Any) -> Any:
        return recursive_transform(
            lambda mulidx, field: cls(
                future,
                mulidx,
            ),
            data_api,
        )

    def result(self):
        return recursive_get(self.future.result(), self.mulidx)
