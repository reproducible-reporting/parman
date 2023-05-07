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

import random
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

FIRST_LINE = "Parman sbatch wait log format version 1"
DEFAULT_FN_LOG = "sbatchwait.log"
DEBUG = False


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
    status = read_step(previous_lines)
    if status is None:
        # A new job must be submitted.
        sbatch_stdout = submit_job(sbatch_args)
        log_step(path_log, f"Submitted {sbatch_stdout}")
    else:
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
    # Here, we take a random sleep time between 30 and 60 seconds to play nice.
    last_status = None
    while True:
        status = read_step(previous_lines)
        if status is None:
            # Random sleep to avoid many scontrol calls at the same time.
            if DEBUG:
                sleep_seconds = 1
                print(f"# Sleep {sleep_seconds}")
            else:
                sleep_seconds = random.randint(30, 60)
            sleep(sleep_seconds)
            # Call scontrol and parse its response.
            status = wait_for_status(jobid, cluster)
            if DEBUG:
                print(f"# {status}")
            if status is not None and status != last_status:
                log_step(path_log, status)
        if status not in ["PENDING", "CONFIGURING", "RUNNING"]:
            return
        if status is not None:
            last_status = status


HELP_MESSAGE = """\
Submit a job with sbatch only once. Wait for it to complete (or fail).

All command-line arguments are passed to sbatch, with `--parsable` in front.
Run `sbatch -h` for details.

The screen output shows the current status of the job,
which is also written to and read from a file called `{path_log}`.
(You can only submit one job from a given directory.)
This script will not resubmit a job if `{path_log}` exists.
Remove this log to make this script forget about the initial job submission.

Note that the timestamps in the log file have a low resolution of about 1 minute.
The job state is only checked every 30--60 seconds so as not to overload the Job Scheduler.
Information from `{path_log}` is maximally reused to avoid unnecessary `scontrol` calls.

This script will wait for the job to complete, just like `sbatch --wait`.
Unlike sbatch, it can also wait for a previously submitted job to complete.
This can be useful when the wait script gets killed for some reason.
"""


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
        return None
    line = lines.pop(0)
    words = line.split(maxsplit=1)
    if len(words) != 2:
        raise ValueError(f"Expected a step in log but found line '{line}'.")
    return words[1]


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


def log_info(path_log: Path, info: str):
    """Write relevant info to the log."""
    log_step(path_log, f"(info) {info}")


def parse_sbatch(stdout: str) -> tuple[int, str | None]:
    """Parse the 'parsable' output of sbatch."""
    words = stdout.split(";")
    if len(words) == 1:
        return int(words[0]), None
    if len(words) == 2:
        return int(words[0]), words[1]
    raise ValueError(f"Cannot parse sbatch output: {stdout}")


def wait_for_status(jobid: int, cluster: str | None) -> str | None:
    """Call scontrol to get the status of the job.

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
        None is returned when scontrol fails in a way that is safe to ignore. (Try again later.)
    """
    args = ["scontrol", "show", "job", str(jobid)]
    if cluster is not None:
        args.append(f"--cluster={cluster}")
    cp = subprocess.run(
        args,
        input="",
        capture_output=True,
        text=True,
    )
    if cp.returncode == 0:
        # Check for the expected state(s).
        match = re.search(r"JobState=(?P<state>[A-Z]+)", cp.stdout)
        if match is not None:
            return match.group("state")
    elif "Invalid job id specified" in cp.stderr:
        # If the jobid is unknown, it is assumed that the job
        # has long gone and has completed.
        return "Invalid"
    return None


if __name__ == "__main__":
    main()
