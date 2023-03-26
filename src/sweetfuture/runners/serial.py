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
"""Serial job runner

Required framework features:

- Easy switching between local and remote execution
- Integration with slurm
- Local multiprocessing
- File-based futures (wait for files to be computed, downloaded, etc.)
- Minimal software dependencies (for jobs): something to read json
- Minimal bloat: no crazy databases and heavy servers
- General purpose: not restricted to a specific application


Frameworks to consider:

- Suitable candidates:
    - No framework: simple serial implementation for API definition and debugging.
    - Built-in module of Python: concurrent.futures.ProcessPoolExecutor
    - Parsl: https://github.com/Parsl/parsl
- See: https://github.com/meirwah/awesome-workflow-engines
  See: https://workflows.community/
  Many solutions but few interesting ones.
  Main weaknesses of other libraries:
    - Domain specific (language).
    - Bloat: trying to solve all problems (servers, databases, web interfaces, ...).
    - Assumptions on workflow structure.
    - Assumptions on hardware.
    - Not Python.
    - Too invasive API.
  Coming close:
    - Dask
    - AiiDA

"""

import attrs

from ..clerks.base import ClerkBase
from .base import RunnerBase
from .jobinfo import validate

__all__ = "SerialRunner"


@attrs.define
class SerialRunner(RunnerBase):
    """Just execute everything right away."""

    clerk: ClerkBase = attrs.field()

    def call(self, func, kwargs, kwargs_api, result_api, resources):
        validate("kwarg", kwargs, kwargs_api)
        result = func(**kwargs)
        validate("result", result, result_api)
        return result
