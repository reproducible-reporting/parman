[![pytest](https://github.com/reproducible-reporting/parman/actions/workflows/pytest.yaml/badge.svg)](https://github.com/reproducible-reporting/parman/actions/workflows/pytest.yaml)
[![release](https://github.com/reproducible-reporting/parman/actions/workflows/release.yaml/badge.svg)](https://github.com/reproducible-reporting/parman/actions/workflows/release.yaml)
[![PyPI Version](https://img.shields.io/pypi/v/parman)](https://pypi.org/project/Parman/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/parman)
![LGPL-3 License](https://img.shields.io/github/license/reproducible-reporting/parman)
[![CodeFactor](https://www.codefactor.io/repository/github/reproducible-reporting/parman/badge)](https://www.codefactor.io/repository/github/reproducible-reporting/parman)
[![DeepSource](https://app.deepsource.com/gh/reproducible-reporting/parman.svg/?label=active+issues&show_trend=true&token=eN22Gnf13peqRJe8V7IAVKsg)](https://app.deepsource.com/gh/reproducible-reporting/parman/)

# Parman

At this stage, Parman is an experimental project, so expect a rocky road ahead.

The goal of Parman is to extend `concurrent.futures` (and compatible implementations)
with features that facilitate a transparent implementation of workflows.

- `WaitFuture`: a Future subclass that is "finished" after its dependencies have finished.
  (To be created with `WaitGraph.submit`, which never blocks.)
- `ScheduledFuture`: a Future subclass that submits a Future after its dependencies have finished.
  (To be created with `Scheduler.submit`, which never blocks.)
- Various `Runner` classes, similar to Executors, which dispatch function calls elsewhere.
  The main differences with conventional executors being:
  - Closures are submitted for (remote) execution, which contain more metadata,
    e.g. about (keyword) arguments and return values, than ordinary functions
    The extra metadata offer several advantages...
  - A dry run can be carried out to quickly validate the connectivity of steps in the workflow
    before launching a full scale calculation.
  - Closure arguments may contain futures.
    If `schedule=True` is set, closures are scheduled for later execution
    when not all dependency futures have finished yet.
    (Dependencies are inferred from the arguments and keyword arguments.)
    Otherwise, the runner will block until all required futures have completed.
  - Closure return values are instantiated as much as possible,
    instead of just returning a single future.
    They may contain futures more deeply nested for parts of the return value,
    This makes it easier to submit more closures further down the workflow.

As a result, workflows can be implemented efficiently with relatively simple Python scripts,
mostly hiding the interaction with Future objects.

Other useful features:

- Compatible with Python's built-in Concurrent package and [Parsl](parsl.readthedocs.io).
  (Parls is an optional dependency.)
- Simplicity:
  - Template jobs, for a straightforward migration of existing job scripts.
  - Minimal Python package dependencies.
  - Minimal API.


## Getting started

### Install

```bash
python -m pip install parman
```

### Examples

At this stage, there is no documentation as such.
If you want to learn how to use Parman, check out the [demos](demos/).
If you want to understand the internals, read the source and the docstrings.


## Non-goals

- Support for Dask, because:
  1. The Dask `Future` does not subclass from `concurrent.futures.Future`.
     Supporting dask would imply a lot of extra boilerplate code in Parman.
  2. The Dask `Future` implements only a subset of `concurrent.futures.Future`.
  3. Dask Distributed has a large memory and time overhead.


## Plans

- Simplify usage.
- Add more examples.
- Tutorial.
