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
"""Pythonic interface to (templates for) job scripts with configurable execution.

A job script can be prepared in a template directory with the following elements:

- A file ``jobinfo.py`` describing the metadata needed to schedule the job in the workflow.
- A file ``run`` (or other) with the script to be executed.

The template directory will be copied every time it needs to be executed (with different parameters).
The job script can read its parameters from a file ``kwargs.json`` and must store the essential
results in ``result.json``. Only keyword arguments are allowed to make the arguments more
self-explaining.

The ``jobinfo.py`` can specify three things:

1) ``get_result_mock`` is mandatory.
   This is a function mimicking the Python interface to the job script.
   The arguments must have type hints to infer the API.
   The result must be a "mocked" result with the correct types of the return values.
   References to files, e.g. inputs needed from previous jobs or results from this job,
   are only recognized properly when they are ``Path`` instances from the built-in
   Python ``pathlib`` module.
2) ``resources`` is optional. This is a dictionary specifying resources, which are specific to
   the runner used. Currently, this is only used by the ParslRunner.
3) ``get_parameter_api`` is optional.
   This function takes the same arguments as ``get_result_mock`` and can be used to return
   a more detailed parameters API than what is possible with type hints.
   It can also be useful when the parameters API depends on values in the parameters,
   although this seems to be a rather exotic scenario.
"""

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
from .treeleaf import transform_tree

__all__ = ("job", "structure", "unstructure")


# The following can be removed once cattrs 23 is released:
# See https://github.com/python-attrs/cattrs/issues/81
cattrs.register_structure_hook(Path, lambda d, t: Path(d))
cattrs.register_unstructure_hook(Path, lambda d: str(d))


@attrs.define
class Job(MetaFuncBase):
    """A metafunction implementation of a job script."""

    template: str = attrs.field()
    jobinfo_source: str = attrs.field()
    resources: dict[str, Any] = attrs.field(init=False)
    parameters_api_func: callable = attrs.field(init=False)
    result_mock_func: callable = attrs.field(init=False)

    def __getstate__(self):
        """Return state for pickle"""
        return self.template, self.jobinfo_source

    def __setstate__(self, d):
        """State from pickle"""
        self.template, self.jobinfo_source = d
        self.__attrs_post_init__()

    def __attrs_post_init__(self):
        """Finalize the initializations of a Job."""
        # Execute the jobinfo source to fill in the other attributes.
        ns = {}
        exec(self.jobinfo_source, ns)
        self.resources = ns.get("resources", {})
        self.parameters_api_func = ns.get("get_parameter_api")
        self.result_mock_func = ns["get_result_mock"]

    @classmethod
    def from_template(cls, template):
        """Initialize a job script from a template directory.

        The directory must contain a ``jobinfo.py`` file with the metadata needed to schedule
        the job in SweetFuture. See module-level docstring for more information on this file.
        """
        with open(os.path.join(template, "jobinfo.py")) as f:
            jobinfo_source = f.read()
        return cls(template, jobinfo_source)

    def describe(self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]) -> Any:
        """Short descriptor of the job.

        See ``__call__`` method for parameter documentation.
        """
        return locator

    def __call__(self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]) -> Any:
        """Execute the job unconditionally.

        Parameters
        ----------
        clerk
            A clerk instance for getting files in the work directory and keeping track of the
            job output files.
        locator
            A locator for the job execution, defining the location of the in and output files in
            the overall workflow.
        script
            The name of the script to be executed.
            This name is also used for stdout and stderr files, by appending ``.out`` and ``.err``,
            respectively.
        kwargs
            Keyword arguments to be stored in the ``kwargs.json`` input for the jobs cript.

        Returns
        -------
        result
            The structured contents of the ``result.json`` file created by the job script.
            Output files needed by following jobs must be included here.
        """

        def run(workdir):
            print(f"Starting {locator}")
            shutil.copytree(self.template, workdir, dirs_exist_ok=True)
            with open(os.path.join(workdir, "kwargs.json"), "w") as f:
                json.dump(unstructure(clerk.localize(kwargs, workdir, locator)), f)
            fn_out = os.path.join(workdir, f"{script}.out")
            fn_err = os.path.join(workdir, f"{script}.err")
            try:
                with open(fn_out, "w") as fo, open(fn_err, "w") as fe:
                    subprocess.run(
                        f"./{script}",
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
                exc.add_note(f"Script {locator} failed: {script}.")
                raise exc
            return True

        result = self._in_workdir(run, clerk, locator, kwargs)
        if result is NotImplemented:
            raise OSError(f"No result.json after completion of {locator}")
        print(f"Completed {locator}")
        return result

    def cached_result(
        self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]
    ) -> Any:
        """Return the result from a previous run if available.

        See ``__call__`` method for parameter documentation.

        Returns
        -------
        result
            The result that ``__call__`` would give with running any job
            (if previous results can be loaded) or ``NotImplemented`` otherwise.
            The existing results are only returned when the kwargs on disk match
            the ones given here. Any inconsistency in inputs implies execution needed.
        """

        def run(workdir):
            fn_kwargs = os.path.join(workdir, "kwargs.json")
            if not os.path.isfile(fn_kwargs):
                return False
            with open(os.path.join(workdir, "kwargs.json")) as f:
                found_kwargs = json.load(f)
            expected_kwargs = unstructure(clerk.localize(kwargs, workdir, locator))
            if found_kwargs != expected_kwargs:
                return False
            return True

        return self._in_workdir(run, clerk, locator, kwargs)

    def _in_workdir(
        self, run: callable, clerk: ClerkBase, locator: str, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Internal method for running something in a job work directory."""
        result_api = type_api_from_mock(self.result_mock_func(**kwargs))
        with clerk.workdir(locator) as workdir:
            fn_result = os.path.join(
                workdir, clerk.localize(locator / Path("result.json"), workdir, locator)
            )
            success = run(workdir)
            if success and os.path.exists(fn_result):
                with open(fn_result) as f:
                    result = structure("result", json.load(f), result_api)
                    return clerk.globalize(result, workdir, locator)
            return NotImplemented

    def get_parameters_api(
        self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Return the parameter API derived from the ``jobinfo.py`` metadata.

        See ``__call__`` method for parameter documentation.
        """
        parameters_api = MetaFuncBase.get_parameters_api(self)
        if self.parameters_api_func is None:
            parameters_api["kwargs"] = type_api_from_signature(
                inspect.signature(self.result_mock_func)
            )
        else:
            parameters_api["kwargs"] = self.parameters_api_func(**kwargs)
        return parameters_api

    def get_result_mock(
        self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]
    ) -> Any:
        """Return a mock result, derived from the ``jobinfo.py`` metadata.

        See ``__call__`` method for parameter documentation.
        """
        return self.result_mock_func(**kwargs)

    def get_resources(
        self, clerk: ClerkBase, locator: str, script: str, kwargs: dict[str, Any]
    ) -> dict:
        """Return the value of the resources dictionary in ``jobinfo.py``

        See ``__call__`` method for parameter documentation.
        """
        return self.resources


def structure(prefix, json_data, data_api):
    """Structure the unstructured data loaded from a JSON file.

    Parameters
    ----------
    prefix
        A prefix referring to the file that was loaded, only used for generating error messages.
    json_data
        The JSON data loaded from a file.
    data_api
        A tree matching the hierarchy of the JSON data with types or type hints that can be
        used to instantiate objects using the JSON data for the constructor.
        Note that `cattrs` is used for the heavy lifting and that all the machinery from `cattrs`
        is at your disposal here. See https://catt.rs/en/stable/

    Returns
    -------
    structured
        A structured copy of the JSON data.
    """

    def transform(mulidx, json_leaf, leaf_api):
        if not isinstance(leaf_api, (type, types.GenericAlias)):
            raise TypeError(f"{prefix} at {mulidx}: cannot structure type {leaf_api}")
        try:
            return cattrs.structure(json_leaf, leaf_api)
        except cattrs.IterableValidationError as exc:
            raise TypeError(
                f"{prefix} at {mulidx}: {json_leaf} does not conform {leaf_api}"
            ) from exc
        except cattrs.StructureHandlerNotFoundError as exc:
            raise TypeError(
                f"{prefix} at {mulidx}: type {leaf_api} cannot be instantiated"
            ) from exc

    return transform_tree(transform, json_data, data_api)


def unstructure(data):
    """Unstructure structured data.

    This is the inverse of ``structure`` and returns a JSON-able result.
    """
    return transform_tree(lambda _, leaf: cattrs.unstructure(leaf), data)


@attrs.define
class JobFactory:
    """Convenience class for instantiating new jobs"""

    clerk: ClerkBase = attrs.field()
    script: str = attrs.field(default="run")
    _cache: dict[str, Job] = attrs.field(init=False, default=attrs.Factory(dict))

    def __call__(self, template, locator, **kwargs):
        """Create a new job with the locator and keyword arguments."""
        job = self._cache.get(template)
        if job is None:
            job = Job.from_template(template)
            self._cache[template] = job
        return Closure(job, [self.clerk, locator, self.script, kwargs])


job = JobFactory(LocalClerk())
