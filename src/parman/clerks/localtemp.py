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
import shutil
import tempfile
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path

import attrs

from .base import ClerkBase

__all__ = ("LocalTempClerk",)


@attrs.define
class LocalTempClerk(ClerkBase):
    """A clerk for calculations carried out in a temporary directory with local storage."""

    root: Path = attrs.field(default=Path("results"))
    tmp: Path = attrs.field(default=Path("tmp"))
    suffix: str = attrs.field(default="")
    prefix: str = attrs.field(default="")

    @contextmanager
    def workdir(self, locator: Path | str) -> Generator[Path, None, None]:
        """See Clerckbase.workdir"""
        if self.tmp is not None:
            self.tmp.mkdir(parents=True, exist_ok=True)
        # Not using the TemporaryDirectory, so files are keept when an exception is raised.
        tmpdir = tempfile.mkdtemp(self.suffix, self.prefix, self.tmp)
        workdir = Path(tmpdir) / locator
        workdir.mkdir(parents=True, exist_ok=True)
        yield workdir
        shutil.rmtree(tmpdir)

    def pull(self, global_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """See Clerckbase.pull"""
        path_src = self.root / global_path
        local_path = Path(os.path.relpath(global_path, locator))
        path_dst = workdir / local_path
        try_copy(path_src, path_dst)
        return local_path

    def push(self, local_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """See Clerckbase.push"""
        path_src = workdir / Path(local_path)
        global_path = locator / Path(local_path)
        path_dst = self.root / global_path
        try_copy(path_src, path_dst)
        return global_path


def try_copy(path_src: Path, path_dst: Path):
    """Try to copy something or fail silently."""
    path_dst.parent.mkdir(parents=True, exist_ok=True)
    with suppress(OSError):
        shutil.copytree(path_src, path_dst)
        return
    with suppress(OSError):
        shutil.copy(path_src, path_dst)
