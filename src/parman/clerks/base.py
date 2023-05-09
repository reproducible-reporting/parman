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
"""Base class for mapping between local and global file structure."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import attrs

from ..treeleaf import transform_tree

__all__ = ("ClerkBase",)


@attrs.define
class ClerkBase:
    """Base class for local-global translation of paths.

    Subclasses implement the three abstract methods below:
    ``workdir``, ``exists``, ``pull`` and ``push``.

    This class contains two pairs of methods that are the inverses of each other:
    - ``localize`` and ``globalize``
    - ``pull`` and ``push``.
    """

    @contextmanager
    def workdir(self, locator: str) -> Generator[Path, None, None]:
        """Initialize the work directory.

        Parameters
        ----------
        locator
            A path-like specification of the location of the data
            of a job within the overall workflow.
        """
        raise NotImplementedError

    def pull(self, global_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """Make sure a global file is present in the work directory.

        Parameters
        ----------
        global_path
            A Path to the file in the global workflow namespace.
        locator
            The locator of the current job, used to construct the local_path.
        workdir
            The work directory where the job is executed.

        Returns
        -------
        local_path
            The (relative) path to the global file from the work directory.
        """
        raise NotImplementedError

    def push(self, local_path: Path | str, locator: Path | str, workdir: Path | str) -> Path:
        """Make sure a local file is made available globally.

        Parameters
        ----------
        local_path
            A local path of a file to be added to the global namespace.
        locator
            The locator of the current job, used to construct the global_path.
        workdir
            The work directory where the job is executed.

        Returns
        -------
        global_path
            The global path of the stored file.
        """
        raise NotImplementedError

    def localize(self, data: Any, locator: Path | str, workdir: Path | str) -> Any:
        """Make sure the files mentioned in data are reachable from the workdir.

        Parameters
        ----------
        data
            The parameters needed for some work in the current directory.
            Whenever Path objects are present (nested in lists or dictionaries),
            they are assumed to be global paths in the workflow,
            and it is guaranteed that the corresponding files are made available.
        locator
            A path-like specification of the location of the data of the current job.
        workdir
            The work directory where the job is executed.

        Returns
        -------
        localized_data
            A transformed copy of data, in which all Path objects are rewritten.
            They now represent file paths relative to the current directory.
        """
        return transform_tree(
            lambda _, leaf: self.pull(leaf, locator, workdir) if isinstance(leaf, Path) else leaf,
            data,
        )

    def globalize(self, data: Any, locator: Path | str, workdir: Path | str) -> Any:
        """Make sure the files mentioned in data become accessible to the whole workflow.

        Parameters
        ----------
        data
            The results produced in the workdir.
            Whenever Path objects are present (nested in lists or dictionaries),
            they are assumed to be relative paths to the current directory,
            and it is guaranteed that they are made available globally for subsequent jobs.
        locator
            A path-like specification of the location of the data of the current job.
        workdir
            The work directory where the job is executed.

        Returns
        -------
        globalized_data
            A transformed copy of data, in which all Path objects are rewritten.
            They now represent file paths in the global workflow.
        """
        return transform_tree(
            lambda _, leaf: self.push(leaf, locator, workdir) if isinstance(leaf, Path) else leaf,
            data,
        )
