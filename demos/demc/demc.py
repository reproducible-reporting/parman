#!/usr/bin/env python
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
"""Differential Evolution Monte Carlo solver for the ill-conditioned regression problem.

This script demonstrates has Parman can be used in a pure Python implementation.
Comment the three decorators below to revert this script back to its serial form.
"""

import argparse
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor

import cattrs
import linreg
import naivemc
import numpy as np
from numpy.typing import NDArray
from parman.runners.concurrent import ConcurrentRunner
from parman.wrapper import wrap

# Taken from https://github.com/python-attrs/cattrs/issues/194
# This is needed to support NumPy arrays in cattrs.
cattrs.register_structure_hook_func(
    lambda t: getattr(t, "__origin__", None) is np.ndarray,
    lambda v, t: np.array([t.__args__[1].__args__[0](e) for e in v]),
)


def main():
    """The Main program."""
    args = parse_args()
    demc(args.parman, args.maxeval, args.burnin)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser("DEMC Demo")
    parser.add_argument("maxeval", default=100000, type=int, help="Maximum function evaluations.")
    parser.add_argument(
        "burnin", default=500, type=int, help="Number of recorded iterations to discard."
    )
    parser.add_argument("-p", "--parman", default=False, action="store_true")
    return parser.parse_args()


def demc(use_parman: bool, maxeval: int, burnin: int):
    """Demonstrate DEMC.

    Parameters
    ----------
    use_parman
        Whether or not to use Parman for parallelization.
    """
    # Problem definition
    eps = 0.1
    rng = np.random.default_rng(seed=1)
    xs, dm, ev = linreg.define_problem(100, 21, eps, rng)
    logp = naivemc.LogPosterior(xs, dm, ev, eps)
    pars_init = np.zeros(dm.shape[1])

    # Parallelization
    if use_parman:
        runner = ConcurrentRunner(executor=ProcessPoolExecutor(max_workers=5))
        my_mc_chain = wrap(runner, mc_chain, mock_mc_chain)
        my_plot_traj = wrap(runner, plot_traj)
        my_analyse_traj = wrap(runner, analyse_traj)
    else:
        my_mc_chain = mc_chain
        my_plot_traj = plot_traj
        my_analyse_traj = analyse_traj

    # DEMC
    nwalker = 5
    histories = [[pars_init] for _ in range(nwalker)]
    trajs_par = [[] for _ in range(nwalker)]
    trajs_lp = [[] for _ in range(nwalker)]
    num_eval = 0
    iouter = 0
    while num_eval < maxeval:
        stride = int(10 * 2 ** (iouter / 200))
        new_points = []
        for i0, history0 in enumerate(histories):
            other_history = []
            if len(history0) > 2:
                for i1, history1 in enumerate(histories):
                    if i0 != i1:
                        other_history.extend(history1)
            par, lp = my_mc_chain(history0[-1], logp, num_eval, other_history, stride)
            trajs_par[i0].append(par)
            trajs_lp[i0].append(lp)
            new_points.append(par)
            num_eval += stride
        for new_point, history in zip(new_points, histories, strict=True):
            history.append(new_point)
            if len(history) > 100:
                del history[0]
        print(f"{num_eval:7d} {stride:4d} {len(histories[-1]):4d}")
        iouter += 1

    # Analysis
    for iwalker, (traj_par, traj_lp) in enumerate(zip(trajs_par, trajs_lp, strict=True)):
        my_plot_traj(f"demc_traj_{iwalker}.png", burnin, traj_par, traj_lp)
    my_analyse_traj(burnin, trajs_par, dm, xs, ev, eps)

    # Wair for everything to end
    if use_parman:
        runner.shutdown()


def mock_mc_chain(
    xa: NDArray[np.float64],
    logp: Callable,
    seed: int,
    xs_other: list[NDArray[np.float64]],
    numiter: int,
) -> list:
    """A mock implementation of the MC chain, from which Parman infers the API."""
    return [np.zeros_like(xa), 1.0]


def mc_chain(
    xa: NDArray[np.float64],
    logp: Callable,
    seed: int,
    xs_other: list[NDArray[np.float64]],
    numiter: int,
) -> list:
    """Run a serial MC chain with an (optional) fixed history from other walkers.

    Parameters
    ----------
    xa
        Initial point.
    logp
        Natural logarithm of the posterior probability, up to a constant.
    seed
        A seed to initialize the RNG.
    xs_other
        A list of historical samples from other walkers.
    numiter
        The number of MC iterations.

    Returns
    -------
    xb
        The last point of the MC chain.
    lpb
        The log posterior at the last point.
    """
    rng = np.random.default_rng(seed)
    lpa = logp(xa)
    gamma = 2.38 / np.sqrt(2 * len(xa))
    for _imc in range(numiter):
        delta = rng.normal(0, len(xa) ** -0.5, xa.shape)
        delta *= 0.02 / rng.normal(0, 1)
        if len(xs_other) > 0:
            x0, x1 = rng.choice(xs_other, 2, False)
            delta += gamma * (x1 - x0)
        xb = xa + delta
        lpb = logp(xb)
        if lpb > lpa or rng.uniform(0, 1) < np.exp(lpb - lpa):
            xa = xb
            lpa = lpb
    return [xa, lpa]


def plot_traj(fn_png: str, burnin: int, traj_x: list[NDArray[np.float64]], traj_lp: list[float]):
    """Wrapper around naivemc.plot_traj"""
    naivemc.plot_traj(fn_png, burnin, np.array(traj_x), np.array(traj_lp))


def analyse_traj(
    burnin: int,
    trajs_par: list[list[NDArray[np.float64]]],
    dm: NDArray[np.float64],
    xs: NDArray[np.float64],
    ev: NDArray[np.float64],
    eps: float,
):
    """Wrapper around naivemc.analyse_traj."""
    traj_pars = np.concatenate([t[burnin:] for t in trajs_par])
    naivemc.analyse_traj("demc", traj_pars, dm, xs, ev, eps)


if __name__ == "__main__":
    main()
