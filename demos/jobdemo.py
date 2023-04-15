#!/usr/bin/env python3
"""Minimalistic SweetFuture demo."""

import parsl

from sweetfuture.job import job
from sweetfuture.runners.concurrent import ConcurrentRunner
from sweetfuture.runners.dry import DryRunner
from sweetfuture.runners.parsl import ParslRunner
from sweetfuture.runners.serial import SerialRunner

# from concurrent.futures import ProcessPoolExecutor

FRAMEWORK = "concurrent"
SCHEDULE = True

BOOT_SIZE = 50
SAMPLE_SIZE = 50
COMMITTEE_SIZE = 10
TEMPERATURES = [300, 600, 900]
NUM_GENERATIONS = 5


def sample(runner, igen, models):
    """Use the model from the previous generation to generate more configurations."""
    configs = []
    for temperature in TEMPERATURES:
        configs.extend(
            runner(
                job(
                    "templates/sample",
                    f"g{igen:02d}/sample/{temperature:04.0f}",
                    sample_size=SAMPLE_SIZE,
                    temperature=temperature,
                    models=models,
                )
            )
        )
    return configs


def compute(runner, igen, configs):
    """Compute reference data for the configurations from the current iteration."""
    for iconfig, config in enumerate(configs):
        yield runner(job("templates/compute", f"g{igen:02d}/compute/{iconfig:04d}", config=config))


def train(runner, igen, examples):
    """Use all training data collected so far to improve the model."""
    return [
        runner(
            job(
                "templates/train", f"g{igen:02d}/train/{imodel:02d}", seed=imodel, examples=examples
            )
        )
        for imodel in range(COMMITTEE_SIZE)
    ]


def setup_runner():
    if FRAMEWORK == "dry":
        return DryRunner()
    elif FRAMEWORK == "serial":
        return SerialRunner()
    elif FRAMEWORK == "concurrent":
        return ConcurrentRunner(schedule=SCHEDULE)
        # return ConcurrentRunner(executor=ProcessPoolExecutor(max_workers=8))
    elif FRAMEWORK == "parsl-local":
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
        return ParslRunner(schedule=SCHEDULE, dfk=parsl.load(config))
    elif FRAMEWORK == "parsl-slurm":
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
        return ParslRunner(schedule=SCHEDULE, dfk=parsl.load(config))
    else:
        raise ValueError("invalid framework")


def main():
    """Main function defining the job workflow."""
    runner = setup_runner()
    examples = []
    configs = runner(job("templates/boot", "g00/boot", boot_size=BOOT_SIZE))
    examples.extend(compute(runner, 0, configs))
    models = train(runner, 0, examples)
    for igen in range(1, NUM_GENERATIONS):
        configs = sample(runner, igen, models)
        examples.extend(compute(runner, igen, configs))
        models = train(runner, igen, examples)
    runner.shutdown()


if __name__ == "__main__":
    main()
