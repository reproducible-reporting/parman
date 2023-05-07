# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
