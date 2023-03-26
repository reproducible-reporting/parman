# SweetFuture enables transparent parallelization.
# Copyright (C) 2023 Toon Verstraelen
#
# This file is part of SweetFuture.
#
# SweetFuture is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# SweetFuture is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>
#
# --


import os
import shutil
import subprocess
import sys
from typing import Any

import attrs

from ..clerks.base import ClerkBase
from .jobinfo import JobInfo

__all__ = ("RunnerBase", "jobfunc")


@attrs.define
class RunnerBase:
    """Just execute everything right away."""

    def job(self, template: str, locator: str, **kwargs):
        jobinfo = JobInfo.from_template(template)
        # TODO: We may add checks for changes in the inputs.
        #       To make this work, hashes of all files are needed.
        kwargs_api, result_api = jobinfo.get_apis(kwargs)
        if self.clerk.has_result(locator):
            return self.clerk.fetch_result(locator, result_api)
        else:
            job_kwargs = {
                "template": template,
                "locator": locator,
                "kwargs": kwargs,
                "result_api": result_api,
                "clerk": self.clerk,
            }
            job_kwargs_api = {
                "template": str,
                "locator": str,
                "kwargs": kwargs_api,
                "result_api": Any,
                "clerk": ClerkBase,
            }
            return self.call(jobfunc, job_kwargs, job_kwargs_api, result_api, jobinfo.resources)

    def call(self, func, kwargs, kwargs_api, result_api, resources):
        """Validate kwargs, execute function, validate result."""
        raise NotImplementedError

    def wait(self):
        """Wait until all jobs have completed."""
        pass


def jobfunc(template, locator, kwargs, result_api, clerk):
    """Execute the job unconditionally."""
    with clerk.jobdir(locator) as jobdir:
        print("Starting", locator)
        shutil.copytree(template, jobdir, dirs_exist_ok=True)
        clerk.write_json_kwargs(kwargs, jobdir, locator)
        fn_out = os.path.join(jobdir, "job.out")
        fn_err = os.path.join(jobdir, "job.err")
        try:
            with open(fn_out, "w") as fo, open(fn_err, "w") as fe:
                subprocess.run(
                    "./run",
                    stdin=subprocess.DEVNULL,
                    stdout=fo,
                    stderr=fe,
                    shell=True,
                    cwd=jobdir,
                    check=True,
                )
        except subprocess.CalledProcessError as e:
            with open(fn_err) as f:
                sys.stderr.write(f.read())
            e.add_note(f"Run script failed for {locator}.")
            raise e
        result = clerk.load_json_result(jobdir, locator, result_api)
    return result
