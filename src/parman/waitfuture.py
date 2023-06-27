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
"""Generic dependency tracking for Future objects.

See `../demo/waitdemo.py` for a simple usage example.

The implementation is compatible with any Future implementation using `concurrent.futures.Future`
as a base class.
As a result, WaitFuture instances can also be used as dependencies.
"""

from collections.abc import Callable, Collection
from concurrent.futures import Future
from threading import Lock

import attrs

__all__ = ("WaitFuture", "WaitGraph")


class WaitFuture(Future):
    """A future Waiting for other futures to finish.

    Users should not create instances manually.
    Use `WaitGraph.submit` instead.
    """

    _digest: Callable
    _dependencies: tuple[Future]

    def __init__(self, dependencies: Collection[Future], digest=None):
        """Initialize a WaitFuture.

        Parameters
        ----------
        dependencies
            A list of other futures that need to finish before this one is finished.
        digest
            A Digest function to process the results of all dependencies.
            When not given, result() will return None.
        """
        super().__init__()
        if not all(isinstance(future, Future) for future in dependencies):
            raise TypeError("All dependencies must be Future instances.")
        self._digest = digest
        self._dependencies = tuple(dependencies)

    def set_state(self):
        """Call Future.set_result with the outcome of the digest function.

        This should not be called by users.
        It is called by WaitGraph after all dependencies have finished.
        """
        results = []
        for future in self._dependencies:
            if future.cancelled():
                results.append(None)
            else:
                exc = future.exception()
                if exc is None:
                    if self._digest is not None:
                        results.append(future.result())
                else:
                    self.set_exception(exc)
                    return
        self.set_result(None if self._digest is None else self._digest(*results))


@attrs.define
class WaitGraph:
    """A Waiter used to detect when dependencies of one or more wait_futures have finished.

    Internally, the directed acyclic graph (DAG) of unfinished wait_futures and their dependencies
    are represented by two dictionaries. As soon as futures finish, they are removed from the
    internals to minimize memory consumption.

    Internal attributes
    -------------------
    _lock
        A lock that is set when modifying the _before and _after attributes.
    _before
        A map from a wait_future to a set of dependency futures.
        (Finished wait_futures are removed.)
    _after
        A map from a dependency future to all wait_futures waiting for
        the dependency to complete.
        (Finished dependencies are removed.)
    """

    _lock: Lock = attrs.field(init=False, default=attrs.Factory(Lock))
    _before: dict[WaitFuture, set[Future]] = attrs.field(init=False, default=attrs.Factory(dict))
    _after: dict[Future, set[WaitFuture]] = attrs.field(init=False, default=attrs.Factory(dict))

    def submit(
        self, dependencies: Collection[Future], digest: Callable | None = None
    ) -> WaitFuture:
        """Create and register a new wait_future.

        Note that this does not create a new thread.
        It (only) installs this WaitGraph instances as a waiter of the dependencies.

        Parameters
        ----------
        dependencies
            A list of dependencies that need to be finished before the wait_future may
            receive the FINISHED state.
        digest
            A digest function, taking as arguments the results of the dependencies.
            When given, the result of the wait_future is the return value of the digest function.
            The digest function is executed in the thread of the last finishing dependency.
            It is not executed when one of the dependency futures raises and exception.
            When digest is not provided, the result of the wait_future is None.

        Returns
        -------
        wait_future
            A WaitFuture instance, which is hooked up internally for dependency tracking.

        """
        wait_future = WaitFuture(dependencies, digest)
        if len(dependencies) == 0:
            wait_future.set_state()
        else:
            self._register(wait_future, dependencies)
            for future in set(dependencies):
                future.add_done_callback(self._handle_done_waiting)
        return wait_future

    def _handle_done_waiting(self, future: Future):
        """Update the wait_futures of which all dependencies have finished."""
        for done_wait_future in self._unregister(future):
            done_wait_future.set_state()

    def _register(self, wait_future: WaitFuture, dependencies: Collection[Future]):
        """Register a new wait_future and its dependencies (thread-safe)."""
        with self._lock:
            before = set(dependencies)
            self._before[wait_future] = before
            for future in before:
                self._after.setdefault(future, set()).add(wait_future)

    def _unregister(self, future: Future):
        """Unregister a (finished) future (thread-safe).

        Parameters
        ----------
        future
            The finished future.

        Returns
        -------
        done_wait_futures
            A list of wait_futures whose dependencies have all finished.
        """
        done_wait_futures = []
        with self._lock:
            wait_futures = self._after.pop(future, None)
            # This method can be called multiple times for the same future.
            # When that future is waited for by multiple wait_futures,
            # the callback is added several times.
            # Hence, it must be checked if wait_futures is None.
            if wait_futures is not None:
                for wait_future in wait_futures:
                    before = self._before[wait_future]
                    before.remove(future)
                    if len(before) == 0:
                        done_wait_futures.append(wait_future)
                        del self._before[wait_future]
        return done_wait_futures
