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
from dask.distributed import Client

from ..closure import Closure
from .future import FutureRunnerBase

__all__ = ("DaskRunner",)


@attrs.define
class DaskRunner(FutureRunnerBase):
    """Run jobs asynchronously with ProcessPoolExecutor"""

    client = attrs.field(default=None)

    def __attrs_post_init__(self):
        if self.client is None:
            self.client = Client()

    def _submit(self, closure: Closure):
        submit_kwargs = closure.get_resources().get("dask_submit_kwargs", {})
        return self.client.submit(Closure.validated_call, closure, **submit_kwargs)

    def wait(self):
        """Wait until all jobs have completed."""
        self.client.shutdown()
