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
"""Tools for interfacing job scripts (or templates) to Python functions."""

import inspect
import json
import os
import shutil
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import attrs
import cattrs

from .clerks.base import ClerkBase
from .clerks.local import LocalClerk
from .closure import Closure
from .metafunc import MetaFuncBase, type_api_from_mock, type_api_from_signature
from .recursive import recursive_transform

__all__ = ("job", "structure", "unstructure")


# The following can be removed once cattrs 23 is released:
# See https://github.com/python-attrs/cattrs/issues/81
cattrs.register_structure_hook(Path, lambda d, t: Path(d))
cattrs.register_unstructure_hook(Path, lambda d: str(d))


@attrs.define
class Job(MetaFuncBase):
    """A Job script implementation of a meta function."""

    template: str = attrs.field()
    jobinfo_source: str = attrs.field()
    resources: dict[str, Any] = attrs.field(init=False)
    parameters_api_func: callable = attrs.field(init=False)
    result_mock_func: callable = attrs.field(init=False)

    def __getstate__(self):
        """Return state for pickle"""
        return (self.template, self.jobinfo_source)

    def __setstate__(self, d):
        """State from pickle"""
        self.template, self.jobinfo_source = d
        self.__attrs_post_init__()

    def __attrs_post_init__(self):
        # Execute the jobinfo source to fill in the extra fields.
        ns = {}
        exec(self.jobinfo_source, ns)
        self.resources = ns.get("resources", {})
        self.parameters_api_func = ns.get("get_parameter_api")
        self.result_mock_func = ns["get_result_mock"]

    @classmethod
    def from_template(cls, template):
        with open(os.path.join(template, "jobinfo.py")) as f:
            jobinfo_source = f.read()
        return cls(template, jobinfo_source)

    def describe(self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]) -> Any:
        return locator

    def __call__(self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]) -> Any:
        result_api = type_api_from_mock(self.result_mock_func(**kwargs))
        with clerk.workdir(locator) as workdir:
            print(f"Starting {locator}")
            shutil.copytree(self.template, workdir, dirs_exist_ok=True)
            kwargs = clerk.localize(kwargs, workdir, locator)
            with open(os.path.join(workdir, "kwargs.json"), "w") as f:
                json.dump(unstructure(kwargs), f)
            fn_out = os.path.join(workdir, "job.out")
            fn_err = os.path.join(workdir, "job.err")
            try:
                with open(fn_out, "w") as fo, open(fn_err, "w") as fe:
                    subprocess.run(
                        "./run",
                        stdin=subprocess.DEVNULL,
                        stdout=fo,
                        stderr=fe,
                        shell=True,
                        cwd=workdir,
                        check=True,
                    )
            except subprocess.CalledProcessError as exc:
                with open(fn_err) as f:
                    sys.stderr.write(f.read())
                exc.add_note(f"Run script failed for {locator}.")
                raise exc
            fn_result = os.path.join(
                workdir, clerk.localize(locator / Path("result.json"), workdir, locator)
            )
            if not os.path.exists(fn_result):
                raise OSError(f"No result.json after completion of {locator}")
            with open(fn_result) as f:
                result = structure("result", json.load(f), result_api)
            result = clerk.globalize(result, workdir, locator)
            print(f"Completed {locator}")
        return result

    def cached_result(self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]) -> Any:
        # TODO: Find nice way to reduce redundancy with __call__ method.
        # TODO: Add mechanism to detect inconsistency between old and new kwargs.
        #       In that case, the result should be recompouted and this method should return
        #       NotImplemented
        result_api = type_api_from_mock(self.result_mock_func(**kwargs))
        with clerk.workdir(locator) as workdir:
            fn_result = os.path.join(
                workdir, clerk.localize(locator / Path("result.json"), workdir, locator)
            )
            if os.path.exists(fn_result):
                with open(fn_result) as f:
                    result = structure("result", json.load(f), result_api)
                return clerk.globalize(result, workdir, locator)
        return NotImplemented

    def get_parameters_api(
        self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        parameters_api = MetaFuncBase.get_parameters_api(self)
        if self.parameters_api_func is None:
            parameters_api["kwargs"] = type_api_from_signature(
                inspect.signature(self.result_mock_func)
            )
        else:
            parameters_api["kwargs"] = self.parameters_api_func(**kwargs)
        return parameters_api

    def get_result_mock(self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]) -> Any:
        return self.result_mock_func(**kwargs)

    def get_resources(self, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]) -> dict:
        return self.resources


def structure(prefix, json_data, data_api):
    def transform(mulidx, json_field, field_api):
        if not isinstance(field_api, (type, types.GenericAlias)):
            raise TypeError(f"{prefix} at {mulidx}: cannot structure type {field_api}")
        try:
            return cattrs.structure(json_field, field_api)
        except cattrs.IterableValidationError as exc:
            raise TypeError(
                f"{prefix} at {mulidx}: {json_field} does not conform {field_api}"
            ) from exc
        except cattrs.StructureHandlerNotFoundError as exc:
            raise TypeError(
                f"{prefix} at {mulidx}: type {field_api} cannot be instantiated"
            ) from exc

    return recursive_transform(transform, json_data, data_api)


def unstructure(data):
    return recursive_transform(lambda _, field: cattrs.unstructure(field), data)


@attrs.define
class JobFactory:
    """Convenience class for instantiating new jobs"""

    clerk: ClerkBase = attrs.field()
    _cache: dict[str, Job] = attrs.field(init=False, default=attrs.Factory(dict))

    def __call__(self, template, locator, **kwargs):
        """Create a new job with locator and keyword arguments."""
        job = self._cache.get(template)
        if job is None:
            job = Job.from_template(template)
            self._cache[template] = job
        return Closure(job, [self.clerk, locator, kwargs])


job = JobFactory(LocalClerk())
