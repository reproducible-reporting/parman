#!/usr/bin/env python3
"""Parman demo with job scripts.

This example mimics a simple active learning workflow.
All jobs are bash or python scripts with some silly IO and a sleep call.

In this example, the functions ``main``, ``parse_args`` and ``setup_runner`` are just utilities
to prepare for the actual example.

The functions ``workflow``, ``sample``, ``compute`` and ``train`` are the actual example itself.
"""

import argparse
from concurrent.futures import ProcessPoolExecutor

import parsl
from parman.clerks.localtemp import LocalTempClerk
from parman.job import job
from parman.runners.concurrent import ConcurrentRunner
from parman.runners.dry import DryRunner
from parman.runners.parsl import ParslRunner
from parman.runners.serial import SerialRunner

BOOT_SIZE = 15
SAMPLE_SIZE = 15
COMMITTEE_SIZE = 3
TEMPERATURES = [300, 400]
NUM_GENERATIONS = 3


# The following line defines a global environment variable for all jobs.
# It is stored in `jobenv.sh` for every job, such that it remains clear they were set.
job.env["SOME_NAME"] = "some_value"


def main():
    """Main program."""
    args = parse_args()
    runner = setup_runner(args.framework, args.schedule)
    if args.in_temp:
        job.clerk = LocalTempClerk()
    if args.queue:
        job.script = "submit.sh"
    workflow(runner, args.pause)


def parse_args() -> argparse.Namespace:
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser("Parman demo with job scripts")
    parser.add_argument(
        "framework",
        help="The framework to execute the workflow",
        choices=["dry", "serial", "threads", "processes", "parsl-local", "parsl-slurm"],
    )
    parser.add_argument(
        "-s",
        "--schedule",
        default=False,
        action="store_true",
        help="Enable the Parman scheduler, not relevant for serial and dry.",
    )
    parser.add_argument(
        "-p", "--pause", default=1.0, type=float, help="The time to pause in each job, in seconds."
    )
    parser.add_argument(
        "-t",
        "--in-temp",
        default=False,
        action="store_true",
        help="Run calculations in a temporary directory.",
    )
    parser.add_argument(
        "-q",
        "--queue",
        default=False,
        action="store_true",
        help="Call submit.sh to put the run script on the queue.",
    )
    return parser.parse_args()


def setup_runner(framework, schedule):
    """Setup a Runner instances."""
    if framework == "dry":
        return DryRunner()
    if framework == "serial":
        return SerialRunner()
    if framework == "threads":
        return ConcurrentRunner(schedule=schedule)
    if framework == "processes":
        return ConcurrentRunner(schedule=schedule, executor=ProcessPoolExecutor(max_workers=8))
    if framework == "parsl-local":
        config = parsl.config.Config(
            executors=[
                parsl.executors.HighThroughputExecutor(
                    label="htex_local",
                    provider=parsl.providers.LocalProvider(
                        channel=parsl.channels.LocalChannel(),
                        init_blocks=1,
                        max_blocks=1,
                    ),
                )
            ],
            strategy="none",
        )
        return ParslRunner(schedule=schedule, dfk=parsl.load(config))
    if framework == "parsl-slurm":
        config = parsl.config.Config(
            executors=[
                parsl.executors.HighThroughputExecutor(
                    label="htex_slaking",
                    address=parsl.addresses.address_by_hostname(),
                    max_workers=4,  # parallel tasks per block
                    provider=parsl.providers.SlurmProvider(
                        # A "block" is a single slurm job.
                        channel=parsl.channels.LocalChannel(),
                        init_blocks=2,
                        max_blocks=2,
                        nodes_per_block=1,
                        cores_per_node=4,
                        scheduler_options=" --cluster=slaking",
                        partition="slaking",
                        launcher=parsl.launchers.SrunLauncher(),
                        worker_init="micromamba activate debug",
                    ),
                )
            ],
            strategy="none",
        )
        return ParslRunner(schedule=schedule, dfk=parsl.load(config))
    raise ValueError("invalid framework")


def workflow(runner, pause):
    """Function defining the job workflow.

    Parameters
    ----------
    runner
        The Runner instance used to schedule and execute the jobs.
    pause
        The time to wait in each job, in seconds
    """
    examples = []
    configs = runner(job("templates/boot", "g00/boot", pause=pause, boot_size=BOOT_SIZE))
    examples.extend(compute(runner, pause, 0, configs))
    models = train(runner, pause, 0, examples)
    for igen in range(1, NUM_GENERATIONS):
        configs = sample(runner, pause, igen, models)
        examples.extend(compute(runner, pause, igen, configs))
        models = train(runner, pause, igen, examples)
    runner.shutdown()


def sample(runner, pause, igen, models):
    """Use the model from the previous generation to generate more configurations."""
    configs = []
    for temperature in TEMPERATURES:
        configs.extend(
            runner(
                job(
                    "templates/sample",
                    f"g{igen:02d}/sample/{temperature:04.0f}",
                    pause=pause,
                    sample_size=SAMPLE_SIZE,
                    temperature=temperature,
                    models=models,
                )
            )
        )
    return configs


def compute(runner, pause, igen, configs):
    """Compute reference data for the configurations from the current iteration."""
    for iconfig, config in enumerate(configs):
        yield runner(
            job(
                "templates/compute",
                f"g{igen:02d}/compute/{iconfig:04d}",
                pause=pause,
                config=config,
            )
        )


def train(runner, pause, igen, examples):
    """Use all training data collected so far to improve the model."""
    return [
        runner(
            job(
                "templates/train",
                f"g{igen:02d}/train/{imodel:02d}",
                pause=pause,
                seed=imodel,
                examples=examples,
            )
        )
        for imodel in range(COMMITTEE_SIZE)
    ]


if __name__ == "__main__":
    main()
