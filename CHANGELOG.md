# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.3] - 2024-06-21

### Fixed

- Fix version info.
- Fixed readability of JSON files: indent with two spaces
- Many small cleanups


## [0.4.2] - 2023-10-27

### Fixed

- File locking in the `parman-sbatch-wait` script was not working as expected,
  which is now fixed.
  The built-in [`fnctl`](https://docs.python.org/3/library/fcntl.html) Python module is used instead of `portalocker`.
  This implies that the `parman-sbatch-wait` script will only work on POSIX-compliant
  operating systems.


## [0.4.1] - 2023-10-17

### Fixed

- There was no way to mark a job as resumable, which made it impossible
  to restart workflows with `parman-sbatch-wait` that still have jobs running in the queue.
  By default, Parman will still complain and stop the workflow if `kwargs.json` is present
  without a corresponding `result.json`.
  If the job script is able to identify a running calculation and wait for its completion,
  then add `can_resume = True` to the `jobinfo.py`.
  (This is always recommended when submitting jobs with `parman-sbatch-wait`.)
  A minimalist example can be found in `demos/jobslow`.


## [0.4.0] - 2023-09-01

### Fixed

- Fix mistake in computation of SHA256 hash: now it reads the entire file.
- Update for recent change in parsl (2023.08.28):
  some defaults were removed from `DataFlowKernel.submit` arguments,
  which are now set explicitly in Parman.

### Added

- New utility script to remove unfinished jobs from a results directory: `parman-clean-results`.
- The output of `scontrol show job` is cached in files under `~/.cache/parman`,
  to reduce the number of `scontrol` calls when many jobs are submitted in parallel.
- Environment variable `PARMAN_SBATCH_CACHE_TIMEOUT` can be set to a minimal waiting time between
  two `scontrol` calls in the `parman-sbatch-wait` script.


## [0.3.1] - 2023-06-15

### Fixed

- Fix PyPI mistake. 0.3.0 will be yanked.


## [0.3.0] - 2023-06-15

### Added

- When `kwargs.json` is manually filled with `null`, it is refreshed under the assumption
  that the existing results are consistent with the new `kwargs.json` file.
  This is useful when one is refactoring workflows and one wants to reuse some results of
  a previous run.
- When `kwargs.sha256` is manually removed, it is refreshed for the same reasons as in the
  previous point.

### Fixed

- Fixed mistake in `jobdemo` example (writing `result.extra`).
- Add support for `None` return value in jobs.

### Changed

- Update [`cattrs`](https://github.com/python-attrs/cattrs) dependency to `>=23.1.2`.


## [0.2.0] - 2023-05-07

### Added

- A pure Python usage example (Differential Evolution Monte Carlo).
- The `parman-sbatch-wait` script to facilitate running workflow jobs on a Slurm cluster.
- Support for optional arguments in `jobinfo.py`.
- Global workflow environment variables for jobs can be defined and stored in the results dir.

### Fixed

- Fixed a Python-3.10 specific bug. (thanks to tox).
- Minimal Python version set to 3.10, for modern type hinting support.

### Changed

- Use `LocalClerk` as default, as it is the most logical first step when setting up a workflow.
- API change: names changed in `jobinfo.py`:
  - `get_result_mock` -> `mock`
  - `get_parameters_api` -> `parameters`


## [0.1.0] - 2023-05-02

Initial public release. See `README.md` for some preliminary details.

This release marks no particular milestone and is just intended to get something out of the door.


[Unreleased]: https://github.com/reproducible-reporting/parman
[0.4.3]: https://github.com/reproducible-reporting/parman/releases/tag/v0.4.3
[0.4.2]: https://github.com/reproducible-reporting/parman/releases/tag/v0.4.2
[0.4.1]: https://github.com/reproducible-reporting/parman/releases/tag/v0.4.1
[0.4.0]: https://github.com/reproducible-reporting/parman/releases/tag/v0.4.0
[0.3.1]: https://github.com/reproducible-reporting/parman/releases/tag/v0.3.1
[0.2.0]: https://github.com/reproducible-reporting/parman/releases/tag/v0.2.0
[0.1.0]: https://github.com/reproducible-reporting/parman/releases/tag/v0.1.0
