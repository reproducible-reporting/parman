#!/usr/bin/env python
r"""Simple plastic deformation of initial molecular geometries.

The goal of this script is to go a bit beyond the simple randomization of Cartesian coordinates.
MD is performed on a primitive potential energy surface, inspired by elastic network models.

The only parameter, besides an initial structure, is a cutoff distance.
Pairs of atoms that are (initially) separated more than the cutoff
will not feel any (restoring) forces to their original distances.
However, when they do get closer during the sampling, they may feel a repulsive force.

A smaller cutoff will result in:

- A more flexible the model
- A more random geometry
- Faster sampling


Referenes
---------

The simplicity of the model used in this script does not do justice to the papers that
were used as a source of inspiration for the model. Still, they deserve to be mentioned:

Anharmonic Fues pair potential:

@article{Fues126,
    author = {Fues, E.},
    title = {Das Eigenschwingungsspektrum zweiatomiger Moleküle in der Undulationsmechanik},
    journal = {Annalen der Physik},
    volume = {385},
    number = {12},
    pages = {367-396},
    doi = {10.1002/andp.19263851204},
    year = {1926}
}

@article{Parr1967,
    author = {Parr, Robert G. and Borkman, Raymond F.},
    title = {Chemical Binding and Potential-Energy Functions for Molecules},
    journal = {The Journal of Chemical Physics},
    volume = {46},
    number = {9},
    pages = {3683--3685},
    year = {1967},
    doi = {10.1063/1.1841277}
}


Badger's rule:

@article{Kratzer1920,
    title={Die ultraroten rotationsspektren der halogenwasserstoffe},
    author={Kratzer, Adolf},
    journal={Zeitschrift f{\"u}r Physik},
    volume={3},
    pages={289--307},
    year={1920},
}

@inbook{doi:https://doi.org/10.1002/9783527633272.ch4,
    author = {Kraka, Elfi and Larsson, John Andreas and Cremer, Dieter},
    publisher = {John Wiley & Sons, Ltd},
    isbn = {9783527633272},
    title = {Generalization of the Badger Rule Based on the Use of Adiabatic Vibrational Modes},
    booktitle = {Computational Spectroscopy},
    chapter = {4},
    pages = {105--149},
    doi = {10.1002/9783527633272.ch4},
    year = {2010},
}

"""

import argparse

import ase.data
import ase.io
import ase.neighborlist
import numpy as np
from ase import units
from ase.calculators.calculator import Calculator
from ase.io.trajectory import Trajectory
from ase.md.langevin import Langevin
from ase.md.velocitydistribution import MaxwellBoltzmannDistribution

# Scaling factor for force constants.
FC_FACTOR = 5.0 * units.Hartree / units.Bohr**2


class PlasticCalculator(Calculator):
    """A very very approximate force field parameterized by an initial geometry."""

    implemented_properties = ("energy", "forces")

    def __init__(self, atoms, cutoff):
        """Instantiate a PlasticCalculator."""
        super().__init__()
        idxs0, idxs1, dists, shifts = ase.neighborlist.neighbor_list("ijdS", atoms, cutoff)
        mask = idxs0 < idxs1
        idxs0 = idxs0[mask]
        idxs1 = idxs1[mask]
        dists = dists[mask]
        shifts = shifts[mask]
        self._pairs = (idxs0, idxs1)
        self._dists_eq = dists
        self._shifts = shifts
        # The following is known as Badger's rule:
        self._fcs = FC_FACTOR / dists**3
        self._radii = np.array([ase.data.vdw_radii[atom.number] for atom in atoms]) * 0.9
        self.atoms = atoms.copy()  # for caching of results

    def calculate(self, atoms, properties, system_changes):
        """Calculate the potential energy and the forces."""
        self.atoms = atoms.copy()  # for caching of results
        energy, forces_x = _calculate_low(
            atoms, self._pairs, self._dists_eq, self._shifts, self._fcs, self._radii
        )
        self.results["energy"] = energy
        self.results["forces"] = forces_x


def _calculate_low(atoms, pairs, dists_eq, shifts, fcs, radii):
    """Directly compute the energy and forces."""
    # Initialize a few things
    atcoords = atoms.get_positions()
    forces = np.zeros_like(atcoords)
    energy = 0

    # Local spring-like interactions
    deltas_a = atcoords[pairs[1]] - atcoords[pairs[0]] + shifts.dot(atoms.cell)
    dists_a = np.linalg.norm(deltas_a, axis=1)
    energy += _calculate_springs(pairs, fcs, dists_eq, shifts, dists_a, deltas_a, forces)

    idxs0_r, idxs1_r, dists_r, deltas_r = ase.neighborlist.neighbor_list("ijdD", atoms, radii)
    mask = idxs0_r < idxs1_r
    idxs0_r = idxs0_r[mask]
    idxs1_r = idxs1_r[mask]
    dists_r = dists_r[mask]
    deltas_r = deltas_r[mask]
    # Compute the repulsive interaction between all pairs.
    energy += _calculate_softballs(1.0, (idxs0_r, idxs1_r), radii, dists_r, deltas_r, forces)
    # Exclude the pairs for which we have springs.
    energy += _calculate_softballs(-1.0, pairs, radii, dists_a, deltas_a, forces)

    return energy, forces


def _calculate_springs(pairs, fcs, dists_eq, shifts, dists_a, deltas_a, forces):
    """Always-active springs (within cutoff initially)"""
    # Harmonic springs are ok-ish.
    # energy += 0.5 * (fcs * (dists_a - dists_eq)**2).sum()
    # derivs_a = fcs * (dists_a - dists_eq)

    # Anharmonic springs are better, model by Fues
    energy = 0.5 * (fcs * dists_eq**4 * (1 / dists_a - 1 / dists_eq) ** 2).sum()
    derivs_a = -fcs * dists_eq**4 * (1 / dists_a - 1 / dists_eq) / dists_a**2
    _chain_rule(forces, pairs, derivs_a, deltas_a, dists_a)
    return energy


def _calculate_softballs(prefac, pairs, radii, dists_r, deltas_r, forces):
    """Short-range repulsion term"""
    dists_c = radii[pairs[0]] + radii[pairs[1]]
    y = (dists_r - dists_c) / dists_r
    energy = prefac * 0.5 * (y**2).sum()
    derivs_r = prefac * y * dists_c / dists_r**2
    _chain_rule(forces, pairs, derivs_r, deltas_r, dists_r)
    return energy


def _chain_rule(forces, pairs, derivs, deltas, dists):
    """Convert distance derivatives into Cartesian forces."""
    pair_forces = (-derivs / dists).reshape(-1, 1) * deltas
    np.add.at(forces, pairs[0], -pair_forces)
    np.add.at(forces, pairs[1], pair_forces)


def main():
    """Main program."""
    args = parse_args()
    simulate(
        args.initial,
        args.traj,
        args.final,
        args.cutoff,
        args.temperature,
        args.steps,
        args.stride,
    )


def simulate(fn_initial, fn_traj, fn_final, cutoff, temperature, steps, stride):
    """Pythonic interface to the main program."""
    atoms = ase.io.read(fn_initial)
    atoms.calc = PlasticCalculator(atoms, cutoff)
    MaxwellBoltzmannDistribution(atoms, temperature_K=2 * temperature)
    dyn = Langevin(atoms, 0.5 * units.fs, friction=0.01, temperature_K=temperature)

    def log():
        """Write terminal output."""
        time = dyn.get_time() / (1000 * units.fs)
        epot = atoms.get_potential_energy()
        ekin = atoms.get_kinetic_energy()
        temp = 2 * ekin / (3 * len(atoms) * units.kB)
        etot = epot + ekin
        print(
            f"Time [ps] = {time:.2f}   Epot [eV] = {epot:.3f}   Ekin [eV] = {ekin:.3f}"
            f"   T [K] = {temp:.3f}   Etot [eV] = {etot:.3f}"
        )

    dyn.attach(log, interval=stride)

    traj = Trajectory(fn_traj, "w", atoms)
    try:
        dyn.attach(traj.write, interval=stride)
        dyn.run(steps)
        ase.io.write(fn_final, atoms)
    finally:
        traj.close()


def parse_args():
    """Parse the command-line arguments."""
    parser = argparse.ArgumentParser("Randomize an atomic structure in a semi-intelligent way.")
    parser.add_argument("initial", help="The initial structure")
    parser.add_argument("traj", help="The output trajectory")
    parser.add_argument("final", help="The final structure")
    parser.add_argument(
        "-c", "--cutoff", default=2.5, type=float, help="Bond cutoff radius. default=%(default)s"
    )
    parser.add_argument(
        "-t",
        "--temperature",
        default=300.0,
        type=float,
        help="Langevin MD temperature. default=%(default)s",
    )
    parser.add_argument(
        "-n", "--steps", default=10000, type=int, help="Number of MD steps. default=%(default)s"
    )
    parser.add_argument(
        "-s",
        "--stride",
        default=100,
        type=int,
        help="Stride between trajectory snapshots. default=%(default)s",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
