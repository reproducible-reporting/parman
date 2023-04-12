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
"""Dry runner, for testing the workflow API."""

from typing import Any

import attrs

from ..closure import Closure
from .base import RunnerBase

__all__ = ("DryRunner",)


@attrs.define
class DryRunner(RunnerBase):
    """Just check inputs and generate example outputs."""

    def __call__(self, closure: Closure) -> Any:
        print(f"Validating {closure.describe()}")
        closure.validate_parameters()
        return closure.get_result_mock()
