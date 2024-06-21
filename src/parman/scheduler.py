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
"""Schedule futures with dependencies.

See `../demo/scheduledemo.py` for a simple usage example.

The implementation is compatible with any Future implementation using `concurrent.futures.Future`
as a base class.
As a result, also ScheduledFuture instances can also be used as dependencies.
"""

import weakref
from collections.abc import Callable, Collection, Mapping
from concurrent.futures import Future
from queue import SimpleQueue
from threading import Lock, Thread
from types import TracebackType

import attrs

from .waitfuture import WaitFuture, WaitGraph

__all__ = ("ScheduledFuture", "Scheduler")


class ScheduledFuture(Future):
    """A future scheduled to be submitted after its dependencies finish.

    Users should not create instances manually.
    Use `Scheduler.submit` instead.
    """

    _args: Collection
    _kwargs: Mapping

    def __init__(self, args: Collection, kwargs: Mapping):
        """Initialize a ScheduledFuture instance.

        Parameters
        ----------
        args
            Arguments to the user_submit function
        kwargs
            Keyword arguments to the user_submit function
        """
        super().__init__()
        self._args = args
        self._kwargs = kwargs


def _submit_loop(scheduler_reference):
    """Submit loop running in a separated thread."""

    def keep_going() -> bool:
        """Single iteration in the submit loop."""
        scheduler = scheduler_reference()
        scheduled_future = scheduler._todo_queue.get(block=True)
        if scheduled_future is None:
            return False
        work_future = scheduler.user_submit(*scheduled_future._args, **scheduled_future._kwargs)
        with scheduler._lock:
            scheduler._work_map[work_future] = scheduled_future
            scheduler._back_map[scheduled_future] = work_future
        work_future.add_done_callback(scheduler._handle_work_done)
        return True

    # Loop with a local namespace for each iteration:
    while keep_going():
        pass


@attrs.define
class Scheduler:
    """A Scheduler for futures.

    Attributes
    ----------
    user_submit
        A function taking *args and **kwargs given to the schedule.user_submit function.
    wait_graph
        A WaitGraph needed for scheduling futures.
        When not given, a new one is created.

    Notes
    -----
    When writing a user_submit function, it should do three things:

    1) Get results from dependency futures.
    2) Submit a function to an executor, using the results as arguments.
    3) Return the future.

    Do not use Future.result() inside the function submitted to the executor.
    Instead, call the result() method before submitting to the executor.

    The scheduler always calls the `user_submit` function from the same thread (the submit_loop).
    To avoid race conditions:

    - Do not call user_submit externally while the scheduler is active.
    - Make sure the user_submit function does not work with or change data that may also be
      used in other threads, most notably the main thread.
      If such data access is needed, use locking mechanisms.

    """

    # Submit a future with given *args and **kwargs.
    user_submit: Callable = attrs.field()
    # wait_graph used wait for multiple dependencies.
    wait_graph: WaitGraph = attrs.field(default=attrs.Factory(WaitGraph))

    # Locks for safe manipulation of the queues.
    _lock: Lock = attrs.field(init=False, default=attrs.Factory(Lock))
    # Background thread submitting scheduled futures
    _submit_thread: Thread = attrs.field(init=False, default=None)
    _shutdown: bool = attrs.field(init=False, default=False)
    # Internals nomenclature:
    # - wait = waiting for dependencies to complete with a WaitFuture
    # - todo = scheduled_futures to to be submitted (in a separate thread).
    # - work = submitted, waiting to finish the actual work
    # - back = lookup the wait_future or work_future of a scheduled_future
    _wait_map: dict[Future:ScheduledFuture] = attrs.field(init=False, default=attrs.Factory(dict))
    _todo_queue: SimpleQueue = attrs.field(init=False, default=attrs.Factory(SimpleQueue))
    _work_map: dict[Future:ScheduledFuture] = attrs.field(init=False, default=attrs.Factory(dict))
    _back_map: dict[ScheduledFuture:Future] = attrs.field(init=False, default=attrs.Factory(dict))

    def __attrs_post_init__(self):
        self._submit_thread = Thread(target=_submit_loop, args=(weakref.ref(self),))
        self._submit_thread.start()

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown()

    def submit(
        self, args: Collection, kwargs: Mapping, dependencies: Collection[Future] | None = None
    ) -> ScheduledFuture:
        """Schedule a future for later submission to an executor.

        Parameters
        ----------
        args, kwargs
            The `submit` function will be called with *args, **kwargs.
        dependencies
            A list of other Future instances that must finish before the future can be submitted.

        Returns
        -------
        scheduled_future
            A future representing the scheduled function call.
            The `running` state is not set correctly, due to a limitation of concurrent.futures.
            It is not possible to get notified when the state of a future changes to RUNNING,
            so the scheduler cannot pass that information through to the returned scheduled_future.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot submit to the scheduler after shutting down.")

        # Create a wait_future and scheduled_future and hook them up to the internals.
        wait_future = self.wait_graph.submit(dependencies)
        scheduled_future = ScheduledFuture(args, kwargs)
        with self._lock:
            self._wait_map[wait_future] = scheduled_future
            self._back_map[scheduled_future] = wait_future
        scheduled_future.add_done_callback(self._handle_scheduled_done)
        wait_future.add_done_callback(self._handle_wait_done)
        return scheduled_future

    def shutdown(self):
        """Wait for the scheduled futures to be submitted to the executor and shut down."""
        with self._lock:
            self._shutdown = True
        self._check_stop_submit_loop()
        self._submit_thread.join()

    def _check_stop_submit_loop(self):
        """Check for shutdown and stop the submit_loop if appropriate."""
        # If there is nothing scheduled for submission to the executor,
        # the submit_loop can be instructed to stop.
        with self._lock:
            if self._shutdown and len(self._wait_map) == 0:
                self._todo_queue.put(None)

    def _handle_wait_done(self, wait_future: WaitFuture):
        """Handle a completed wait_future: submit the corresponding scheduled_future."""
        with self._lock:
            scheduled_future = self._wait_map.pop(wait_future, None)
            del self._back_map[scheduled_future]
        if wait_future.cancelled():
            scheduled_future.cancel()
        elif not scheduled_future.cancelled():
            exc = wait_future.exception()
            if exc is None:
                self._todo_queue.put(scheduled_future)
            else:
                scheduled_future.set_exception(exc)
        self._check_stop_submit_loop()

    def _handle_work_done(self, work_future: Future):
        """Handle a completed work_future: assign result to the corresponding scheduled_future."""
        with self._lock:
            scheduled_future = self._work_map.pop(work_future, None)
            del self._back_map[scheduled_future]
        if work_future.cancelled():
            scheduled_future.cancel()
        elif not scheduled_future.cancelled():
            exc = work_future.exception()
            if exc is None:
                scheduled_future.set_result(work_future.result())
            else:
                scheduled_future.set_exception(exc)

    def _handle_scheduled_done(self, scheduled_future: ScheduledFuture):
        """Handle a completed scheduled_future, only relevant when cancelled by the user."""
        if scheduled_future.cancelled():
            with self._lock:
                other_future = self._back_map.pop(scheduled_future, None)
                if other_future is None:
                    return
                self._wait_map.pop(other_future, None)
                self._work_map.pop(other_future, None)
            other_future.cancel()
        self._check_stop_submit_loop()
