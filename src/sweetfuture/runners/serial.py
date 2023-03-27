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
"""Serial job runner, mainly useful for debugging, not using any Future instances."""

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
