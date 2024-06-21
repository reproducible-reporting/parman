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
"""Run the demos to verify that they run without errors.

If the files in the jobdemo are updated, refresh the hashes as follows:

(
    cd demos/jobdemo && ./jobdemo.py serial -p 0 &&
    find results -type f | grep -v -E '(jobenv.sh|submit.sh)' | \
     xargs sha256sum > ../../tests/jobdemo-results.sha256
)
"""

import hashlib
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def test_linreg(tmp_path: Path):
    run_script([sys.executable, "linreg.py"], Path("demos", "demc"), [Path("linreg.py")], tmp_path)


def test_naivemc(tmp_path: Path):
    run_script(
        [sys.executable, "naivemc.py", "1000", "10"],
        Path("demos", "demc"),
        [Path("linreg.py"), Path("naivemc.py")],
        tmp_path,
    )


def test_demc_serial(tmp_path: Path):
    run_script(
        [sys.executable, "demc.py", "1000", "10"],
        Path("demos", "demc"),
        [Path("linreg.py"), Path("naivemc.py"), Path("demc.py")],
        tmp_path,
    )


def test_demc_parman(tmp_path: Path):
    run_script(
        [sys.executable, "demc.py", "1000", "10", "--parman"],
        Path("demos", "demc"),
        [Path("linreg.py"), Path("naivemc.py"), Path("demc.py")],
        tmp_path,
    )


@pytest.mark.parametrize(
    ("framework", "schedule", "in_temp"),
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
def test_jobdemo(framework: str, schedule: bool, in_temp: bool, tmp_path: Path):
    args = [sys.executable, "jobdemo.py", framework, "--pause=0"]
    if schedule:
        args.append("-s")
    if in_temp:
        args.append("-t")

    relpaths = [
        Path("jobdemo.py"),
        Path("templates", "boot", "jobinfo.py"),
        Path("templates", "boot", "run"),
        Path("templates", "boot", "script.py"),
        Path("templates", "compute", "jobinfo.py"),
        Path("templates", "compute", "run"),
        Path("templates", "sample", "jobinfo.py"),
        Path("templates", "sample", "run"),
        Path("templates", "sample", "script.py"),
        Path("templates", "train", "helper.py"),
        Path("templates", "train", "jobinfo.py"),
        Path("templates", "train", "run"),
    ]

    # Run the jobdemo in a subprocess. (runpy does not work with parsl.)
    run_script(args, Path("demos", "jobdemo"), relpaths, tmp_path)
    if framework != "dry":
        check_files(tmp_path, Path("tests", "jobdemo-results.sha256"))


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


def test_plastic_ibuprofen(tmp_path: Path):
    run_script(
        [
            sys.executable,
            "plastic.py",
            "ibuprofen.xyz",
            "ibuprofen.traj",
            "ibuprofen_final.xyz",
            "--steps=100",
        ],
        Path("demos", "inspiration", "plastic"),
        [Path("plastic.py"), Path("ibuprofen.xyz")],
        tmp_path,
    )


def test_plastic_alumina(tmp_path: Path):
    run_script(
        [
            sys.executable,
            "plastic.py",
            "alumina.json",
            "alumina.traj",
            "alumina_final.xyz",
            "--steps=100",
        ],
        Path("demos", "inspiration", "plastic"),
        [Path("plastic.py"), Path("alumina.json")],
        tmp_path,
    )


def run_script(args: list[str], root: Path, relpaths: list[Path], tmp_path: Path, nrepeat: int = 1):
    """Run a script with auxiliary files in a temporary directory."""
    # Put the files in place
    for relpath in relpaths:
        dn_dst = tmp_path / relpath.parent
        dn_dst.mkdir(parents=True, exist_ok=True)
        shutil.copy(root / relpath, tmp_path / relpath)
    # Execute the script
    env = os.environ | {"PYTHONPATH": Path("src").absolute()}
    for _ in range(nrepeat):
        subprocess.run(args, check=True, cwd=tmp_path, env=env)


@pytest.mark.parametrize(
    "path",
    [
        Path("demos", "mindmutable.py"),
        Path("demos", "scheduledemo.py"),
        Path("demos", "waitdemo.py"),
    ],
)
def test_simpledemo(path):
    runpy.run_path(path)
