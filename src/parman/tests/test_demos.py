# ParMan extends Python concurrent.futures to facilitate parallel workflows.
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
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Run the demos to verify that they run without errors."""
import hashlib
import importlib.resources
import os
import runpy
import shutil
import subprocess
from pathlib import Path

import pytest

JOBDEMO_RELPATHS = [
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


@pytest.mark.parametrize(
    "framework, schedule, in_place",
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
def test_jobdemo(framework: str, schedule: bool, in_place: bool, tmppath: Path):
    # Put the files of the jobdemo in place
    for relpath in JOBDEMO_RELPATHS:
        dn_dst = tmppath / relpath.parent
        dn_dst.mkdir(parents=True, exist_ok=True)
        shutil.copy(Path("demos/jobdemo-al") / relpath, tmppath / relpath)

    # Run the jobdemo in a subprocess. (runpy does not work with parsl.)
    args = ["python3", "jobdemo.py", framework, "--pause=0"]
    if schedule:
        args.append("-s")
    if in_place:
        args.append("-i")
    env = os.environ | {"PYTHONPATH": Path("src/").absolute()}
    for _ in range(2):
        subprocess.run(args, check=True, cwd=tmppath, env=env)
        if framework != "dry":
            check_files(tmppath, "jobdemo-al-results.sha256")


def check_files(root, fn_sha):
    with importlib.resources.files("parman.tests").joinpath(fn_sha).open("r") as f:
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
