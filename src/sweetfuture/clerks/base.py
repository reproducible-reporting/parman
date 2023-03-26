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
"""Base class for clerks"""

import os

import attrs

from ..runners.jobinfo import JobInfo

__all__ = "ClerkBase"


# Frozen is used to make the objects immutable,
# which is needed for Parsl's HTEX.


@attrs.frozen
class ClerkBase:
    def jobdir(self, locator: str):
        raise NotImplementedError

    def write_json_kwargs(self, kwargs: dict, jobdir: str, locator: str):
        raise NotImplementedError

    def has_result(self, jobdir: str) -> bool:
        raise NotImplementedError

    def fetch_result(self, locator: str, result_api: dict):
        raise NotImplementedError

    def load_json_result(self, jobdir: str, locator: str, result_api: dict):
        raise NotImplementedError
