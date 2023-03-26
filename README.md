# SweetFuture

At this stage, SweetFuture is a prototype, so expect a rocky road ahead.

## Design goals

- Wrap `futures` APIs, completely hiding parallelization technicalities.
  Workflows are written as if they are serial.
- Compatibility with other futures-based frameworks like Dask and Parsl.
- Simplicity:
  - Lower learning curve than Dask or Parsl.
  - Template jobs, for a straightforward migration of existing job scripts.
  - Minimal dependencies
  - Minimal API.
  - Job dependencies are automatically tracked through future results,
    which are pushed as deep as possible in nested results,
    hiding them from the workflow code as much as possible.

## Future plans

- Write a better README.
