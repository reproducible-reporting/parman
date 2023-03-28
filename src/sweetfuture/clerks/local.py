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
"""Local to global mapping for in-tree calculations."""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import attrs

from ..recursive import recursive_transform
from .base import ClerkBase

__all__ = ("LocalClerk",)


@attrs.define
class LocalClerk(ClerkBase):
    root: str = attrs.field(default="results")

    @contextmanager
    def workdir(self, locator: str):
        workdir = os.path.join(self.root, locator)
        if not os.path.exists(workdir):
            os.makedirs(workdir)
        yield workdir

    def localize(self, data: Any, jobdir: str, locator: str) -> Any:
        def transform(_, field):
            # pathlib is still work in progress, so it seems. :(
            return Path(os.path.relpath(field, locator)) if isinstance(field, Path) else field

        return recursive_transform(transform, data)

    def globalize(self, data: Any, jobdir: str, locator: str) -> Any:
        return recursive_transform(
            lambda _, field: locator / field if isinstance(field, Path) else field, data
        )
