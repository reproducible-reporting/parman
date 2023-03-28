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
"""Dask job runner, wrapper for Dask futures."""

import attrs
from dask.distributed import Client, Future

from ..clerks.base import ClerkBase
from ..clerks.local import LocalClerk
from ..recursive import recursive_get, recursive_transform
from .base import RunnerBase
from .concurrent import FutureResult, validating_wrapper
from .jobinfo import validate

__all__ = ("DaskRunner",)


@attrs.define
class DaskRunner(RunnerBase):
    """Run jobs asynchronously with ProcessPoolExecutor"""

    clerk: ClerkBase = attrs.field(default=attrs.Factory(LocalClerk))
    client = attrs.field(default=None)

    def __attrs_post_init__(self):
        if self.client is None:
            self.client = Client()

    def call(self, func, kwargs, kwargs_api, result_api, resources):
        kwargs = recursive_transform(
            lambda _, field: field.result() if isinstance(field, FutureResult) else field,
            kwargs,
        )
        validate("kwarg", kwargs, kwargs_api)
        submit_kwargs = resources.get("dask_submit_kwargs", {})
        future = self.client.submit(validating_wrapper, func, kwargs, result_api, **submit_kwargs)
        return recursive_transform(
            lambda mulidx, field: FutureResult(future, mulidx),
            result_api,
        )

    def wait(self):
        """Wait until all jobs have completed."""
        self.client.shutdown()
