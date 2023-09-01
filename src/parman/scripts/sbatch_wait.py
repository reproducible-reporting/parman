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
"""An sbatch wrapper to submit only on the first call, and to wait until a job has finished."""

import os
import random
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import portalocker

FIRST_LINE = "Parman sbatch wait log format version 1"
SCONTROL_FAILED = "The command `scontrol show job` failed!\n"
DEFAULT_FN_LOG = "sbatchwait.log"
DEBUG = False
CACHE_TIMEOUT = int(os.getenv("PARMAN_SBATCH_CACHE_TIMEOUT", "30"))
POLLING_INTERVAL = int(os.getenv("PARMAN_SBATCH_POLLING_INTERVAL", "10"))
TIME_MARGIN = int(os.getenv("PARMAN_SBATCH_TIME_MARGIN", "5"))


def main(path_log=None):
    """Main program."""
    if path_log is None:
        path_log = Path(DEFAULT_FN_LOG)
    sbatch_args = parse_args(path_log)
    submit_once_and_wait(path_log, sbatch_args)


def submit_once_and_wait(path_log, sbatch_args):
    # Read previously logged steps
    previous_lines = []
    if path_log.is_file():
        print(f"Reading from {path_log}")
        with open(path_log) as f:
            check_log_version(next(f).strip())
            for line in f:
                line = line.strip()
                print("SKIP", line)
                previous_lines.append(line)
    else:
        print(f"Creating new {path_log}")
        with open(path_log, "w") as f:
            f.write(FIRST_LINE + "\n")

    # Go through or skip steps.
    submit_time, status = read_step(previous_lines)
    if status is None:
        # A new job must be submitted.
        submit_time = time.time()
        sbatch_stdout = submit_job(sbatch_args)
        log_step(path_log, f"Submitted {sbatch_stdout}")
        rndsleep()
    else:
        # The first step, if present in the log, is the submission.
        step, sbatch_stdout = status.split()
        if step != "Submitted":
            raise ValueError(f"Expected 'Submitted' in log, found '{step}'")
    jobid, cluster = parse_sbatch(sbatch_stdout)

    # Wait for the job to complete
    # The polling loop below is discouraged in the Slurm documentation,
    # yet this is also how the `sbatch --wait` command works internally.
    # See https://bugs.schedmd.com/show_bug.cgi?id=14638
    # The maximum sleep time between two calls in `sbatch --wait` is 32 seconds.
    # See https://github.com/SchedMD/slurm/blob/master/src/sbatch/sbatch.c
    # Here, we take a random sleep time, by default between 30 and 60 seconds to play nice.
    last_status = None
    while True:
        # First try to replay previously logged steps
        status_time, status = read_step(previous_lines)
        if status is None:
            # All previously logged steps are processed.
            # Call scontrol and parse its response.
            rndsleep()
            status_time, status = get_status(jobid, cluster)
            if DEBUG:
                print(f"# {status}")
            if status is not None and status != last_status:
                log_step(path_log, status)
        if (status_time > submit_time + TIME_MARGIN) and (
            status not in ["PENDING", "CONFIGURING", "RUNNING"]
        ):
            return
        if status is not None:
            last_status = status


HELP_MESSAGE = """\
Submit a job with sbatch only once. Wait for it to complete (or fail).

All command-line arguments are passed to sbatch, with `--parsable` in front.
Run `sbatch -h` for details.

This script will wait for the job to complete, just like `sbatch --wait`.
Unlike `sbatch --wait`, it can also wait for a previously submitted job to complete.
This can be useful when the wait script gets killed for some reason.

The screen output shows the current status of the job,
which is also written to and read from a file called `{path_log}`.
(You can only submit one job from a given directory.)
This script will not resubmit a job if `{path_log}` exists.
Instead, it will wait for the job to complete without resubmission.
Remove `{path_log}` to make this script forget about the initial job submission.

Note that the timestamps in the log file have a low resolution of about 1 minute.
The job state is only checked every 30--40 seconds so as not to overload the Job Scheduler.
Information from `{path_log}` is maximally reused to avoid unnecessary `scontrol` calls.

The status of the job is inferred from `scontrol show job`, if relevant with a `--cluster`
argument. To further minimize the number of `scontrol` calls in a parallel workflow,
its output is cached and stored under `~/.cache/parman`. The cached results are reused by
all instances of this script, such that the number of `scontrol` calls is independent of the
number of jobs running in parallel.

The time between two `scontrol` calls (per cluster) can be controlled with the
environment variable `PARMAN_SBATCH_CACHE_TIMEOUT`, which is "30" (seconds) by default.
Increase this value if you want to reduce the burden on Slurm.

The cached output of `scontrol` is checked with a randomized polling interval.
The randomization guarantees that concurrent calls to `scontrol` (for multiple clusters)
do not all coincide.
The polling time can be controlled with two additional environment variables:

- `PARMAN_SBATCH_POLLING_INTERVAL` = the minimal polling interval in seconds, default is "10".
- `PARMAN_SBATCH_TIME_MARGIN` = the with of the uniform distribution for the polling interval
  in seconds, default is "5"."""


def parse_args(path_log):
    """Parse command-line arguments."""
    args = sys.argv[1:]
    if any(arg in ["-?", "-h", "--help"] for arg in args):
        print(HELP_MESSAGE.format(path_log=path_log))
        sys.exit(2)
    return args


def check_log_version(line: str):
    """Validate the log version, abort if there is a mismatch."""
    if line != FIRST_LINE:
        raise ValueError(
            "The first line of the log is wrong. " f"Expected: '{FIRST_LINE}' " f"Found: '{line}'"
        )


def read_step(lines: list[str]) -> str | None:
    """Read a step from the log file."""
    if len(lines) == 0:
        return None, None
    line = lines.pop(0)
    words = line.split(maxsplit=1)
    if len(words) != 2:
        raise ValueError(f"Expected a step in log but found line '{line}'.")
    return datetime.fromisoformat(words[0]).timestamp(), words[1]


def rndsleep():
    """Randomized sleep to distribute I/O load evenly."""
    if DEBUG:
        sleep_seconds = 1
    else:
        sleep_seconds = random.randint(POLLING_INTERVAL, POLLING_INTERVAL + TIME_MARGIN)
    time.sleep(sleep_seconds)


def submit_job(args: list[str]) -> str:
    """Submit a job with sbatch."""
    # Put --parsable in front
    args = ["sbatch", "--parsable"] + [arg for arg in args if arg != "--parsable"]
    cp = subprocess.run(
        args,
        input="",
        stdout=subprocess.PIPE,
        check=True,
        text=True,
    )
    return cp.stdout.strip()


def log_step(path_log: Path, step: str):
    """Write a step to the log."""
    dt = datetime.now().isoformat()
    with open(path_log, "a") as f:
        line = f"{dt} {step}"
        print(f"LOG  {line}")
        f.write(f"{line}\n")


def parse_sbatch(stdout: str) -> tuple[int, str | None]:
    """Parse the 'parsable' output of sbatch."""
    words = stdout.split(";")
    if len(words) == 1:
        return int(words[0]), None
    if len(words) == 2:
        return int(words[0]), words[1]
    raise ValueError(f"Cannot parse sbatch output: {stdout}")


def get_status(jobid: int, cluster: str | None) -> str | None:
    """Load cached scontrol output or run scontrol if outdated.

    Parameters
    ----------
    jobid
        The job to wait for.
    cluster
        The cluster to which the job was submitted.

    Returns
    -------
    status
        A status reported by scontrol.
        The "Invalid" status is returned when scontrol fails to find the jobid.
        None is returned when scontrol fails in a way that is safe to ignore.
        (Try again later.)
    """
    # Load cached output or run again
    args = ["scontrol", "show", "job"]
    path_out = Path(os.getenv("HOME")) / ".cache/parman"
    if cluster is None:
        path_out /= "sbatch_wait.out"
    else:
        args.append(f"--cluster={cluster}")
        path_out /= f"sbatch_wait.{cluster}.out"
    status_time, scontrol_out = cached_run(args, path_out, CACHE_TIMEOUT)
    return status_time, parse_scontrol_out(scontrol_out, jobid)


def cached_run(args: list[str], path_out: Path, cache_timeout) -> str:
    """Execute a command if its previous output is outdated.

    Parameters
    ----------
    args
        List of command line arguments (including executable).
    path_out
        The path where the output is cached.
    cache_timeout
        The waiting time between two actual calls.

    Returns
    -------
    stdout
        The output of the file, either new or cached.

    Notes
    -----
    The cached output is updated only if the command has a zero exit code.
    In all other cases, the result of the call is ignored, assuming the error is transient.
    """
    if not path_out.exists():
        path_out.parent.mkdir(parents=True, exist_ok=True)
        path_out.touch(exist_ok=True)

    with portalocker.Lock(path_out, mode="r+") as fh:
        header = fh.read(CACHE_HEADER_LENGTH)
        cache_time, _ = parse_cache_header(header)
        if cache_time is None or time.time() > cache_time + cache_timeout:
            cp = subprocess.run(args, input="", capture_output=True, text=True, check=False)
            fh.truncate(0)
            cache_time = time.time()
            fh.write(make_cache_header(cache_time, cp.returncode))
            fh.write(cp.stdout)
            return cache_time, cp.stdout
        return cache_time, fh.read()


def make_cache_header(cache_time, returncode):
    iso = datetime.fromtimestamp(cache_time).isoformat()
    assert len(iso) == 26
    return f"v1 datetime={iso} returncode={returncode:+04d}\n"


def parse_cache_header(header):
    if len(header) == 0 or header == "\x00" * CACHE_HEADER_LENGTH:
        return None, None
    elif len(header) == CACHE_HEADER_LENGTH:
        print((header,))
        if not header.startswith("v1 datetime="):
            raise ValueError("Invalid header")
        cache_time = datetime.fromisoformat(header[12:38]).timestamp()
        returncode = int(header[50:54])
        return cache_time, returncode
    else:
        raise ValueError(f"Cannot parse cache header: {header}")


CACHE_HEADER_LENGTH = len(make_cache_header(time.time(), 0))


def parse_scontrol_out(scontrol_out: str, jobid: int) -> str | None:
    """Get the job state for a specific from from the output of ``scontrol show job``.

    Parameters
    ----------
    scontrol_out
        A string with the output of ``scontrol show job``.
    jobid
        The jobid of interest.

    Returns
    -------
    jobstate
        The status of the job, or None of the job cannot be found.
    """
    if scontrol_out == SCONTROL_FAILED:
        return "Invalid"
    match = re.search(
        f"JobId={jobid}.*?JobState=(?P<state>[A-Z]+)",
        scontrol_out,
        flags=re.MULTILINE | re.DOTALL,
    )
    if match is not None:
        return match.group("state")
    return None


if __name__ == "__main__":
    main()
