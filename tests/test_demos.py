# Parman extends Python concurrent.futures to facilitate parallel workflows.
# Copyright (C) 2023 Toon Verstraelen
#
# This file is part of ParMan.
#
# ParMan is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# ParMan is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Run the demos to verify that they run without errors."""


import hashlib
import os
import runpy
import shutil
import subprocess
from pathlib import Path

import pytest


def test_linreg(tmppath: Path):
    run_script(["python3", "linreg.py"], Path("demos/demc"), [Path("linreg.py")], tmppath)


def test_naivemc(tmppath: Path):
    run_script(
        ["python3", "naivemc.py", "1000", "10"],
        Path("demos/demc"),
        [Path("linreg.py"), Path("naivemc.py")],
        tmppath,
    )


def test_demc_serial(tmppath: Path):
    run_script(
        ["python3", "demc.py", "1000", "10"],
        Path("demos/demc"),
        [Path("linreg.py"), Path("naivemc.py"), Path("demc.py")],
        tmppath,
    )


def test_demc_parman(tmppath: Path):
    run_script(
        ["python3", "demc.py", "1000", "10", "--parman"],
        Path("demos/demc"),
        [Path("linreg.py"), Path("naivemc.py"), Path("demc.py")],
        tmppath,
    )


@pytest.mark.parametrize(
    "framework, schedule, in_temp",
    [
        ("dry", False, False),
        ("serial", False, False),
        ("serial", False, True),
        ("threads", False, False),
        ("threads", False, True),
        ("threads", True, False),
        ("threads", True, True),
        ("processes", False, False),
        ("processes", False, True),
        ("processes", True, False),
        ("processes", True, True),
        ("parsl-local", False, False),
        ("parsl-local", False, True),
        ("parsl-local", True, False),
        ("parsl-local", True, True),
    ],
)
def test_jobdemo(framework: str, schedule: bool, in_temp: bool, tmppath: Path):
    args = ["python3", "jobdemo.py", framework, "--pause=0"]
    if schedule:
        args.append("-s")
    if in_temp:
        args.append("-t")

    relpaths = [
        Path("jobdemo.py"),
        Path("templates/boot/jobinfo.py"),
        Path("templates/boot/run"),
        Path("templates/compute/jobinfo.py"),
        Path("templates/compute/run"),
        Path("templates/sample/jobinfo.py"),
        Path("templates/sample/run"),
        Path("templates/train/helper.py"),
        Path("templates/train/jobinfo.py"),
        Path("templates/train/run"),
    ]

    # Run the jobdemo in a subprocess. (runpy does not work with parsl.)
    run_script(args, Path("demos/jobdemo"), relpaths, tmppath)
    if framework != "dry":
        check_files(tmppath, "tests/jobdemo-results.sha256")


def test_plastic_ibuprofen(tmppath: Path):
    run_script(
        [
            "python3",
            "plastic.py",
            "ibuprofen.xyz",
            "ibuprofen.traj",
            "ibuprofen_final.xyz",
            "--steps=100",
        ],
        Path("demos/inspiration/plastic"),
        [Path("plastic.py"), Path("ibuprofen.xyz")],
        tmppath,
    )


def test_plastic_alumina(tmppath: Path):
    run_script(
        [
            "python3",
            "plastic.py",
            "alumina.json",
            "alumina.traj",
            "alumina_final.xyz",
            "--steps=100",
        ],
        Path("demos/inspiration/plastic"),
        [Path("plastic.py"), Path("alumina.json")],
        tmppath,
    )


def run_script(args: list[str], root: Path, relpaths: list[Path], tmppath: Path, nrepeat: int = 1):
    """Run a script with auxiliary files in a temporary directory."""
    # Put the files in place
    for relpath in relpaths:
        dn_dst = tmppath / relpath.parent
        dn_dst.mkdir(parents=True, exist_ok=True)
        shutil.copy(root / relpath, tmppath / relpath)
    # Execute the script
    env = os.environ | {"PYTHONPATH": Path("src/").absolute()}
    for _ in range(nrepeat):
        subprocess.run(args, check=True, cwd=tmppath, env=env)


def check_files(root, fn_sha):
    with open(fn_sha) as f:
        for line in f:
            sha256, path = line.split()
            check_hash(sha256, root / path)


def check_hash(expected_sha256, path):
    with open(path, "rb") as f:
        content = f.read()
        if hashlib.sha256(content).hexdigest() != expected_sha256:
            raise AssertionError(f"SHA256 mismatch: {path}")


@pytest.mark.parametrize(
    "path", ["demos/mindmutable.py", "demos/scheduledemo.py", "demos/waitdemo.py"]
)
def test_simpledemo(path):
    runpy.run_path(path)
