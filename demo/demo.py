#!/usr/bin/env python3
"""Minimalistic SWAT demo."""

import parsl

from sweetfuture.clerks.local import Clerk
from sweetfuture.runners.concurrent import ConcurrentRunner
from sweetfuture.runners.dask import DaskRunner
from sweetfuture.runners.dry import DryRunner
from sweetfuture.runners.parsl import ParslRunner
from sweetfuture.runners.serial import SerialRunner

# from concurrent.futures import ThreadPoolExecutor


FRAMEWOWRK = "concurrent"
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
            runner.job(
                "templates/sample",
                f"g{igen:02d}/sample/{temperature:04.0f}",
                sample_size=SAMPLE_SIZE,
                temperature=temperature,
                models=models,
            )
        )
    return configs


def compute(runner, igen, configs):
    """Compute reference data for the configurations from the current iteration."""
    for iconfig, config in enumerate(configs):
        yield runner.job("templates/compute", f"g{igen:02d}/compute/{iconfig:04d}", config=config)


def train(runner, igen, examples):
    """Use all training data collected so far to improve the model."""
    return [
        runner.job(
            "templates/train", f"g{igen:02d}/train/{imodel:02d}", seed=imodel, examples=examples
        )
        for imodel in range(COMMITTEE_SIZE)
    ]


def main():
    if FRAMEWOWRK == "dry":
        runner = DryRunner()
    elif FRAMEWOWRK == "serial":
        runner = SerialRunner(Clerk())
    elif FRAMEWOWRK == "concurrent":
        runner = ConcurrentRunner(Clerk())
        # runner = ConcurrentRunner(Clerk(), ThreadPoolExecutor(max_workers=8))
    elif FRAMEWOWRK == "parsl-local":
        config = parsl.config.Config(
            executors=[
                parsl.executors.HighThroughputExecutor(
                    label="htex_Local",
                    provider=parsl.providers.LocalProvider(
                        channel=parsl.channels.LocalChannel(),
                        init_blocks=1,
                        max_blocks=1,
                    ),
                )
            ],
            strategy="none",
        )
        runner = ParslRunner(Clerk(), parsl.load(config))
    elif FRAMEWOWRK == "dask":
        runner = DaskRunner(Clerk())
    else:
        raise ValueError("invalid framework")

    examples = []
    configs = runner.job("templates/boot", "g00/boot", boot_size=BOOT_SIZE)
    examples.extend(compute(runner, 0, configs))
    models = train(runner, 0, examples)
    for igen in range(1, NUM_GENERATIONS):
        configs = sample(runner, igen, models)
        examples.extend(compute(runner, igen, configs))
        models = train(runner, igen, examples)
    runner.wait()


if __name__ == "__main__":
    main()
