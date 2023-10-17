#!/usr/bin/env python3
"""Minimalistic example testing restart with running job on queue."""

from parman.job import job
from parman.runners.serial import SerialRunner


def main():
    """Main program."""
    job.script = "submit.sh"
    runner = SerialRunner()
    runner(job("template", "step1"))
    runner(job("template", "step2"))


if __name__ == "__main__":
    main()
