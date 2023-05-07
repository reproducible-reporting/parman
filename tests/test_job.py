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
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --
"""Unit tests for parman.job."""

import os
import stat

from parman.job import JobFactory

ENVVAR_JOBINFO = """
def get_result_mock() -> str:
    return "mock"
"""

ENVVAR_RUN = """\
#!/usr/bin/env python

import json
import os


def main():
    with open("result.json", "w") as f:
        json.dump(os.getenv("PARMAN_TEST_JOB_QOFDHEIC", "wrong"), f)


if __name__ == "__main__":
    main()
"""


def test_env_pass_through(tmppath):
    sample_path = tmppath / "templates" / "sample"
    sample_path.mkdir(parents=True)
    with open(sample_path / "jobinfo.py", "w") as f:
        f.write(ENVVAR_JOBINFO)
    with open(sample_path / "run", "w") as f:
        f.write(ENVVAR_RUN)
    os.chmod(sample_path / "run", stat.S_IRWXU)
    job = JobFactory()
    job.clerk.root = tmppath / "results"
    closure = job(sample_path, "sample")
    os.putenv("PARMAN_TEST_JOB_QOFDHEIC", "correct")
    result = closure.validated_call()
    assert result == "correct"
