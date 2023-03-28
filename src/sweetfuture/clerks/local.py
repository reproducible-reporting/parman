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

import json
import os
from contextlib import contextmanager
from pathlib import Path

import attrs

from ..recursive import recursive_transform
from ..runners.jobinfo import structure, unstructure
from .base import ClerkBase

__all__ = ("LocalClerk",)


@attrs.define
class LocalClerk(ClerkBase):
    work: str = attrs.field(default="work")

    @contextmanager
    def jobdir(self, locator: str):
        jobdir = os.path.join(self.work, locator)
        if not os.path.exists(jobdir):
            os.makedirs(jobdir)
        yield jobdir

    def write_json_kwargs(self, kwargs: dict, jobdir: str, locator: str):
        def transform(_, field):
            # pathlib is still work in progress, so it seems. :(
            return Path(os.path.relpath(field, locator)) if isinstance(field, Path) else field

        json_kwargs = unstructure(recursive_transform(transform, kwargs))
        with open(os.path.join(jobdir, "kwargs.json"), "w") as f:
            json.dump(json_kwargs, f)

    def has_result(self, locator: str) -> bool:
        # TODO: use pathlib as much as possible
        return os.path.isfile(os.path.join(self.work, locator, "result.json"))

    def fetch_result(self, locator: str, result_api: dict):
        return self.load_json_result(os.path.join(self.work, locator), locator, result_api)

    def load_json_result(self, jobdir: str, locator: str, result_api: dict):
        fn_results = os.path.join(jobdir, "result.json")
        if not os.path.isfile(fn_results):
            raise OSError(f"No outputs after completion of {locator}")
        with open(fn_results) as f:
            json_results = json.load(f)
        return recursive_transform(
            lambda _, field: locator / field if isinstance(field, Path) else field,
            structure("result", json_results, result_api),
        )
