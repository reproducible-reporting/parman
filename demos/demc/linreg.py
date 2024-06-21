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
"""Definition and standard solution of an ill-conditioned linear regression"""

import matplotlib.pyplot as plt
import numpy as np

__all__ = ("define_problem", "solve_regression", "plot_solutions")


def main():
    """Main program."""
    eps = 0.1
    rng = np.random.default_rng(seed=1)
    xs, dm, ev = define_problem(100, 20, eps, rng)
    pars_mu, pars_cov = solve_regression(dm, ev, eps)
    plot_solutions("linreg_solution.png", xs, ev, [("linreg", pars_mu, pars_cov)])


def define_problem(
    npoint: int, nbasis: int, eps: float, rng
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Define a prototypical ill-conditioned linear regession problem.

    Parameters
    ----------
    npoint
        The number of points to fit to.
    nbasis
        The number of polynomial basis functions.
    eps
        The standard deviation of the normally distributed measurement noise.
    rng
        A random number generator used to generate data for the fitting problem.

    Returns
    -------
    x
        The grid points on the x-axis.
    dm
        The design matrix, a Vandermonde matrix.
    ev
        The expected values (including noise).
    """
    xs = rng.uniform(-1, 1, npoint)
    ev = np.sqrt(1 - xs**2) + rng.normal(0, eps, npoint)
    dm = xs.reshape(-1, 1) ** np.arange(nbasis)
    return xs, dm, ev


def solve_regression(dm: np.array, ev: np.array, eps: float) -> tuple[np.ndarray, np.ndarray]:
    """Solve a standard multivariate regression problem.

    Parameters
    ----------
    dm
        A design matrix.
    ev
        The expected values (measurements)
    eps
        The standard deviation of the measurement noise.

    Returns
    -------
    pars_mu
        The expected values of the parameters.
    pars_cov
        The covariance of the parameters.
    """
    u, s, vt = np.linalg.svd(dm, full_matrices=False)
    pars_mu = np.dot(vt.T, np.dot(u.T, ev) / s)
    pars_cov = eps**2 * np.einsum("ji,j,jk->ik", vt, 1 / s**2, vt)
    return pars_mu, pars_cov


def plot_solutions(fn_png: str, xs: np.ndarray, ev: np.ndarray, solutions: list):
    """Visualize a part of the regression solution.

    Parameters
    ----------
    fn_png
        A filename for the figure.
    xs
        Grid points on te x-axis.
    ev
        The expected values.
    solutions
        A list of tuples, each (method, pars_mu, pars_cov), which are the
        estimated mean and covariance of the model parameters with some method.
    """

    def plot_fits(ax):
        """Plot the fitted parameters."""
        x_grid = np.linspace(-1, 1, 101)
        for _, pars_mu, _ in solutions:
            y_grid = np.dot(x_grid.reshape(-1, 1) ** np.arange(len(pars_mu)), pars_mu)
            ax.plot(x_grid, y_grid)
        return x_grid

    def plot_curves(ax):
        """Plot the curves described by the parameters."""
        x_grid = plot_fits(ax)
        ax.plot(x_grid, np.sqrt(1 - x_grid**2), color="k")
        ax.set_xlabel("x")
        ax.set_ylabel("y")

    def plot_data(ax):
        """Plot the expected values."""
        plot_fits(ax)
        ax.plot(xs, ev, marker="+", ls="none", color="k")
        ax.set_xlabel("x")
        ax.set_ylabel("y")

    def plot_pars(ax0, ax1):
        """Plot the parameters as a function of the eigenvalue index."""
        for label, pars_mu, pars_cov in solutions:
            evals = np.linalg.eigvalsh(pars_cov)
            sigmas = np.sqrt(abs(evals))
            ax0.plot(sigmas, marker="o", ls="none", label=label)
            ax1.plot(pars_mu, marker="o", ls="none")
        ax0.legend(loc=0)
        ax0.set_ylabel("o = sqrt(Eigenvalue)")
        ax0.set_xlabel("Eigenvalue index")
        ax0.set_yscale("log")
        ax1.set_ylabel("Transformed parameter")
        ax1.set_xlabel("Parameter index")

    fig, axs = plt.subplots(2, 2, figsize=(8, 8), constrained_layout=True)
    plot_curves(axs[0, 0])
    plot_data(axs[0, 1])
    plot_pars(axs[1, 0], axs[1, 1])
    fig.savefig(fn_png, dpi=200)


if __name__ == "__main__":
    main()
