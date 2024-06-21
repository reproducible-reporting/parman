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
"""Abstract future job runner."""

from concurrent.futures import Future, as_completed
from functools import partial
from threading import Lock
from typing import Any

import attrs

from ..closure import Closure
from ..scheduler import Scheduler
from ..treeleaf import get_tree, iterate_tree, transform_tree
from ..waitfuture import WaitGraph
from .base import RunnerBase

__all__ = ("FutureRunnerBase",)


@attrs.define
class FutureRunnerBase(RunnerBase):
    """Abstract base classes for running functions with Futures.

    Subclasses must override `_submit`.
    """

    schedule: bool = attrs.field(default=False)
    wait_graph: WaitGraph = attrs.field(default=attrs.Factory(WaitGraph))
    _scheduler: Scheduler = attrs.field(init=False, default=None)
    _futures: list[Future] = attrs.field(init=False, default=attrs.Factory(list))
    _submit_lock: Lock = attrs.field(init=False, default=attrs.Factory(Lock))

    def __attrs_post_init__(self):
        if self.schedule:
            self._scheduler = Scheduler(self._submit, self.wait_graph)

    def __call__(self, closure: Closure) -> Any:
        if self.schedule:
            dependencies = []
            for data in closure.args, closure.kwargs:
                for _, leaf in iterate_tree(data):
                    if isinstance(leaf, Future):
                        dependencies.append(leaf)
            print(f"Scheduling {closure.describe()} after {len(dependencies)} futures")
            future = self._scheduler.submit([closure], {}, dependencies)
        else:
            future = self._submit(closure)
        self._futures.append(future)
        return _promise_data(future, self.wait_graph, closure.get_result_api())

    def _unpack_data(self, closure):
        """Recursively transform Futures into actual results."""
        if self.schedule:
            _validate_done(closure.describe(), closure.args)
            _validate_done(closure.describe(), closure.kwargs)
        return Closure(
            closure.metafunc,
            _wait_for_data(closure.args),
            _wait_for_data(closure.kwargs),
        )

    def _submit(self, closure: Closure) -> Future:
        """Submit a closure to the executor.

        (The implementation in subclasses must be thread-safe, using self._submit_lock.)
        """
        raise NotImplementedError

    def shutdown(self):
        """Wait for all futures to complete."""
        if self.schedule:
            print("Shutting down the scheduler, waiting for it drain")
            self._scheduler.shutdown()
        else:
            print("Waiting for all futures to finish")
        # Manually wait for futures to complete, to make sure exceptions are shown.
        for future in as_completed(self._futures):
            future.result()
        print("Shutting down the executor")


def _wait_for_data(data: Any) -> Any:
    """Recursively replace Futures by actual results, waiting if needed."""
    return transform_tree(
        lambda _, leaf: leaf.result() if isinstance(leaf, Future) else leaf,
        data,
    )


class FutureNotDoneError(RuntimeError):
    """Raised when results from futures are needed while they are not available yet."""


def _validate_done(needed_for: str, data: Any) -> Any:
    """Validate that all futures (nested recursively) are done."""
    for mulidx, leaf in iterate_tree(data):
        if isinstance(leaf, Future) and not leaf.done():
            raise FutureNotDoneError(
                f"Trying to get an unavailable result for argument {mulidx} of {needed_for}."
            )


def _promise_data(future: Future, wait_graph: WaitGraph, data_api: Any) -> Any:
    """Build a result, recursively inserting Futures for all return values."""
    return transform_tree(
        lambda mulidx, _: wait_graph.submit([future], partial(get_tree, mulidx=mulidx)),
        data_api,
    )
