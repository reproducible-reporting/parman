# Example of `cached_run`

This is a simple example to test the `cached_run` function used by `parman-sbatch-wait`.
The function `cached_run` reduces the number of calls to `scontrol show jobs` by caching the output, which is relevant for highly parallel workflows.
