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
"""Base class for mapping between local and global file structure."""

from contextlib import contextmanager
from typing import Any

import attrs

__all__ = ("ClerkBase",)


@attrs.define
class ClerkBase:
    @contextmanager
    def workdir(self, locator: str):
        raise NotImplementedError

    def localize(self, data: Any, jobdir: str, locator: str) -> Any:
        raise NotImplementedError

    def globalize(self, data: Any, jobdir: str, locator: str) -> Any:
        raise NotImplementedError
