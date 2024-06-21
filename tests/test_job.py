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
"""Unit tests for parman.job."""

import os
import stat
from pathlib import Path

import pytest
from parman.job import JobFactory, strip_line, write_sh_env


def setup_jobfactory(root: Path, jobinfo: str, run: str) -> JobFactory:
    template_path = root / "templates" / "sample"
    template_path.mkdir(parents=True)
    with open(template_path / "jobinfo.py", "w") as f:
        f.write(jobinfo)
    with open(template_path / "run", "w") as f:
        f.write(run)
    os.chmod(template_path / "run", stat.S_IRWXU)
    job = JobFactory()
    job.clerk.root = root / "results"
    return job, template_path


ENVVAR_JOBINFO = """
def mock() -> dict[str, str]:
    return {"aaaa": "", "bbbb": ""}
"""

ENVVAR_RUN = """\
#!/usr/bin/env python

import json
import os


def main():
    assert "PARMAN_WORKDIR" in os.environ
    with open("result.json", "w") as f:
        json.dump({
            "aaaa": os.environ.get("PARMAN_TEST_JOB_AAAA", "wrong_a"),
            "bbbb": os.environ.get("PARMAN_TEST_JOB_BBBB", "wrong_b"),
        }, f)


if __name__ == "__main__":
    main()
"""


def test_envvar(tmp_path: Path):
    job, template_path = setup_jobfactory(tmp_path, ENVVAR_JOBINFO, ENVVAR_RUN)
    job.env = {"PARMAN_TEST_JOB_BBBB": "correct_b"}
    closure = job(template_path, "sample")
    os.environ["PARMAN_TEST_JOB_AAAA"] = "correct_a"
    result = closure.validated_call()
    assert result["aaaa"] == "correct_a"
    assert result["bbbb"] == "correct_b"
    assert len(result) == 2
    with open(tmp_path / "results" / "sample" / "jobenv.sh") as f:
        lines = f.readlines()
    assert lines[0] == 'export PARMAN_TEST_JOB_BBBB="correct_b"\n'
    assert lines[1] == f'export PARMAN_WORKDIR="{os.getcwd()}"\n'
    assert len(lines) == 2


OPTIONAL_JOBINFO = """
def mock(first: int, second: int = 2) -> int:
    return 42
"""

OPTIONAL_RUN = """\
#!/usr/bin/env python

import json
import os


def main():
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    with open("result.json", "w") as f:
        json.dump(kwargs["first"] + kwargs["second"], f)


if __name__ == "__main__":
    main()
"""


def test_optional(tmp_path: Path):
    job, template_path = setup_jobfactory(tmp_path, OPTIONAL_JOBINFO, OPTIONAL_RUN)
    closure = job(template_path, "sample", first=1)
    result = closure.validated_call()
    assert result == 3


@pytest.mark.parametrize(
    ("inp", "out"),
    [
        ("  aaa bbb \n", "aaa bbb"),
        ("  aaa # bbb \n", "aaa"),
        ("  #  aaa # bbb \n", ""),
    ],
)
def test_strip_line(inp, out):
    assert strip_line(inp) == out


def test_write_sh_env(tmp_path: Path):
    path_rc = tmp_path / "test.sh"
    write_sh_env(path_rc, {"ABC": "123", "FOO": "BAR"})
    with open(path_rc) as f:
        lines = f.readlines()
    assert lines[0] == 'export ABC="123"\n'
    assert lines[1] == 'export FOO="BAR"\n'


def test_write_sh_env_exceptions(tmp_path: Path):
    path_rc = tmp_path / "test.sh"
    with pytest.raises(ValueError):
        write_sh_env(path_rc, {"1ABC": "z"})
    with pytest.raises(ValueError):
        write_sh_env(path_rc, {"": "z"})
    with pytest.raises(ValueError):
        write_sh_env(path_rc, {1: "z"})


NONE_JOBINFO = """
def mock(first: int, second: int = 2) -> int:
    return None
"""

NONE_RUN = """\
#!/usr/bin/env python

import json
import os


def main():
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    with open("result.json", "w") as f:
        f.write("null")


if __name__ == "__main__":
    main()
"""


def test_none(tmp_path: Path):
    job, template_path = setup_jobfactory(tmp_path, NONE_JOBINFO, NONE_RUN)
    closure = job(template_path, "sample", first=1, second=3)
    result = closure.validated_call()
    assert result is None


PATH_JOBINFO = """
from pathlib import Path
def mock(some_input: Path) -> Path:
    return Path("__foo__")
"""

PATH_RUN = """\
#!/usr/bin/env python

import json
import os


def main():
    with open("kwargs.json") as f:
        kwargs = json.load(f)

    with open("result.json", "w") as f:
        json.dump("some_output.txt", f)


if __name__ == "__main__":
    main()
"""


def test_path(tmp_path: Path):
    job, template_path = setup_jobfactory(tmp_path, PATH_JOBINFO, PATH_RUN)
    (tmp_path / "results").mkdir()
    with open(tmp_path / "results/some_input.txt", "w") as f:
        f.write("foo bar\n")
    closure = job(template_path, "sample", some_input=Path("some_input.txt"))
    result = closure.validated_call()
    assert result == Path("sample/some_output.txt")
