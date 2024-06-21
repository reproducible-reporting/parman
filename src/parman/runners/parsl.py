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
"""Parsl job runner, wraps around Parsl AppFuture.

From the ``resources`` dictionary attribute of the MetaFunc and Closure, only the
file ``parsl_executors`` is used. (If not present, ``all`` is used by default.)
This is passed into ``parsl.dataflow.dflow.DataFlowKernel.submit``.
"""

from concurrent.futures import Future

import attrs
from parsl.dataflow.dflow import DataFlowKernelLoader

from ..closure import Closure
from .future import FutureRunnerBase

__all__ = ("ParslRunner",)


@attrs.define
class ParslRunner(FutureRunnerBase):
    """Run jobs asynchronously with Parsl"""

    dfk: DataFlowKernelLoader = attrs.field(default=None)

    def __attrs_post_init__(self):
        FutureRunnerBase.__attrs_post_init__(self)
        if self.dfk is None:
            self.dfk = DataFlowKernelLoader.load()

    def _submit(self, closure: Closure) -> Future:
        """Submit a closure to the executor."""
        closure = self._unpack_data(closure)
        executors = closure.get_resources().get("parsl_executors", "all")
        print(f"Submitting {closure.describe()}")
        with self._submit_lock:
            return self.dfk.submit(
                func=Closure.validated_call,
                app_args=[closure],
                executors=executors,
                cache=False,
                ignore_for_cache=[],
                app_kwargs={},
                join=False,
            )

    def shutdown(self):
        """Wait for all futures to complete."""
        FutureRunnerBase.shutdown(self)
        self.dfk.wait_for_current_tasks()
