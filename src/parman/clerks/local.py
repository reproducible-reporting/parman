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
"""Local to global mapping for in-tree calculations."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import attrs

from .base import ClerkBase

__all__ = ("LocalClerk",)


@attrs.define
class LocalClerk(ClerkBase):
    """A clerk for calculations taking place in the directory where the workflow data are stored."""

    root: Path = attrs.field(default=Path("results"))

    @contextmanager
    def workdir(self, locator: Path | str) -> Generator[Path, None, None]:
        """See Clerckbase.workdir"""
        workdir = self.root / locator
        workdir.mkdir(parents=True, exist_ok=True)
        yield workdir

    def pull(self, global_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """See Clerckbase.pull"""
        if workdir != self.root / locator:
            raise RuntimeError(
                "Internal inconsistency in LocalClerck.pull: "
                f"workdir={workdir}, expected={self.root / locator}"
            )
        # pathlib is still work in progress, so it seems. :(
        return Path(os.path.relpath(global_path, locator))

    def push(self, local_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """See Clerckbase.push"""
        if workdir != self.root / locator:
            raise RuntimeError(
                "Internal inconsistency in LocalClerck.push: "
                f"workdir={workdir}, expected={self.root / locator}"
            )
        return Path(locator) / local_path
