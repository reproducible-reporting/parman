# SweetFuture

At this stage, SweetFuture is a prototype, so expect a rocky road ahead.

## Design goals

- Wrap `futures` APIs, completely hiding parallelization technicalities.
  Workflows are written as if they are serial.
  You may still access the `Future` objects, but you generally don't have to.
- Compatibility with other futures-based frameworks like Dask and Parsl.
- Simplicity:
  - Lower learning curve than Dask or Parsl.
  - Template jobs, for a straightforward migration of existing job scripts.
  - Minimal dependencies.
  - Minimal API.
  - Job dependencies are automatically tracked through future results,
    which are pushed as deep as possible in nested results,
    hiding them from the workflow code as much as possible.
    This is similar to Parsl's DataFuture, but with some extra's:
    - A runner can return more than a future, also a nested datastructure containing FutureResult
      instances.
    - The runner can take similar nested structures containing FutureResult instances,
      detects these, waits for their completion and calls the job with the result.

## Future plans

- Write a better README.
