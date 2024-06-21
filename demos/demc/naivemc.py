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
"""Naive Monte Carlo solver for the ill-conditioned regression problem."""

import argparse
from collections.abc import Callable

import attrs
import linreg
import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray

__all__ = ("LogPosterior", "plot_traj", "analyse_traj")


@attrs.define
class LogPosterior:
    """Posterior probability (with flat prior) for a linear regression problem."""

    xs: NDArray[np.float64] = attrs.field()
    dm: NDArray[np.float64] = attrs.field()
    ev: NDArray[np.float64] = attrs.field()
    eps: float = attrs.field()

    def __call__(self, pars: NDArray[np.float64]) -> float:
        mv = np.dot(self.dm, pars)
        return -((mv - self.ev) ** 2).sum() / (2 * self.eps**2)


def main():
    """The Main program."""
    args = parse_args()
    naivemc(args.maxeval, args.burnin)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser("DEMC Demo")
    parser.add_argument("maxeval", default=100000, type=int, help="Maximum function evaluations.")
    parser.add_argument(
        "burnin", default=500, type=int, help="Number of recorded iterations to discard."
    )
    return parser.parse_args()


def naivemc(maxeval: int, burnin: int):
    """The main program."""
    # Problem definition
    eps = 0.1
    rng = np.random.default_rng(seed=1)
    xs, dm, ev = linreg.define_problem(100, 21, eps, rng)
    logp = LogPosterior(xs, dm, ev, eps)
    pars_init = np.zeros(dm.shape[1])

    # Sampling
    traj_pars, traj_lp = mc_chain(pars_init, logp, rng, maxeval, 50)

    # Trajectory
    plot_traj("naivemc_traj.png", burnin, traj_pars, traj_lp)

    # Average parameters and uncertainty
    analyse_traj("naivemc", traj_pars[burnin:], dm, xs, ev, eps)


def mc_chain(
    xa: NDArray[np.float64], logp: Callable, rng: np.random.Generator, numiter: int, stride: int
) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Naive Monte Carlo chain

    Parameters
    ----------
    xa
        Initial point.
    logp
        Natural logarithm of the posterior probability, up to a constant.
    rng
        A NumPy random number generator.
    numiter
        The number of MC iterations.
    stride
        The number of MC steps after which a point is stored in the trajectory.

    Returns
    -------
    traj
        The sampled trajectory.
    """
    lpa = logp(xa)
    traj_x = [xa]
    traj_lp = [lpa]
    num_accept = 0
    for imc in range(numiter):
        delta = rng.normal(0, len(xa) ** -0.5, xa.shape)
        delta *= 0.02 / rng.normal(0, 1)
        xb = xa + delta
        lpb = logp(xb)
        if lpb > lpa or rng.uniform(0, 1) < np.exp(lpb - lpa):
            xa = xb
            lpa = lpb
            num_accept += 1
        if imc % stride == stride - 1:
            print(f"{imc:6d} | {lpa:7.2f} | Acceptance ratio = {num_accept / (imc + 1) * 100:.0f}%")
            traj_x.append(xa)
            traj_lp.append(lpa)

    return np.array(traj_x), np.array(traj_lp)


def plot_traj(fn_png: str, burnin: int, traj_x: NDArray[np.float64], traj_lp: NDArray[np.float64]):
    """Plot the trajectory of an MC chain."""

    def plot_lp(ax):
        """Plot the log posterior probability as a function of MC iteration."""
        ax.plot(traj_lp)
        ax.axvline(burnin, color="k", alpha=0.5)
        ax.set_ylim(traj_lp[burnin:].min(), traj_lp[burnin:].max())
        ax.set_ylabel("Log Posterior")
        ax.set_xlabel("MC Sample")

    def plot_x(ax):
        """Plot the parameters as a function of MC iteration."""
        ax.axvline(burnin, color="k", alpha=0.5)
        for traj_comp in traj_x.T:
            ax.plot(traj_comp)
        ax.set_ylabel("Parameter")
        ax.set_xlabel("MC Sample")

    fig, axs = plt.subplots(1, 2, figsize=(8, 4), constrained_layout=True)
    plot_lp(axs[0])
    plot_x(axs[1])
    fig.savefig(fn_png, dpi=200)


def analyse_traj(
    labelmc: str,
    traj_pars: NDArray[np.float64],
    dm: NDArray[np.float64],
    xs: NDArray[np.float64],
    ev: NDArray[np.float64],
    eps: float,
):
    """Analyze the sampling mean and covariance, and compare to the linear regression result."""
    pars_mu = traj_pars.mean(axis=0)
    pars_cov = np.cov(traj_pars, rowvar=False)
    pars_mu_linreg, pars_cov_linreg = linreg.solve_regression(dm, ev, eps)
    solutions = [
        ("linreg", pars_mu_linreg, pars_cov_linreg),
        (labelmc, pars_mu, pars_cov),
    ]
    linreg.plot_solutions(f"{labelmc}_solution.png", xs, ev, solutions)


if __name__ == "__main__":
    main()
