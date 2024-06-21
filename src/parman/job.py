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
"""Pythonic interface to (templates for) job scripts with configurable execution.

A job script can be prepared in a template directory with the following elements:

- A file ``jobinfo.py`` describing the metadata needed to schedule the job in the workflow.
- A file ``run`` (or other) with the script to be executed.

The template directory will be copied to a working directory, every time a job is executed
(with different parameters). Before, the job is executed, a file ``kwargs.json`` is created
in the work directory from which the job script can read it parameters. Only keyword arguments
are allowed to make the arguments more self-explaining. The job writes its results to
``result.json``. If the jobs fails, not writing this file will raise an exception and end the
workflow (after other running jobs have completed).

The ``jobinfo.py`` can specify three things:

1) ``mock`` is mandatory.
   This is a function mimicking the Python interface to the job script.
   The arguments must have type hints to infer the API.
   The result must be a "mocked" result with the correct types of the return values.
   References to files, e.g. inputs needed from previous jobs or results from this job,
   are only recognized properly when they are ``Path`` instances from the built-in
   Python ``pathlib`` module.
2) ``resources`` is optional. This is a dictionary specifying resources, which are specific to
   the runner used. Currently, this is only used by the ParslRunner.
3) ``parameters`` is optional.
   This function takes the same arguments as ``mock`` and can be used to return
   a more detailed parameters API than what is possible with type hints.
   It can also be useful when the parameters API depends on values in the parameters,
   although this seems to be a rather exotic scenario.

Other files in the work directory that may be relevant or useful:

- ``kwargs.sha256``: SHA256 sums of all files present in ``kwargs.json``, used to check if the
  results can be reused without recomputation.
- ``result.extra``: A list of files that are also worth keeping, even though they are not
  mentioned in ``result.json``.
- ``run.out``: the standard output of the run script.
- ``run.err``: the standard error of the run script.
"""

import hashlib
import inspect
import json
import os
import re
import shutil
import string
import subprocess
import sys
import types
from collections.abc import Callable
from pathlib import Path
from types import NoneType
from typing import Any

import attrs
import cattrs

from .clerks.base import ClerkBase
from .clerks.local import LocalClerk
from .closure import Closure
from .metafunc import MetaFuncBase, type_api_from_mock, type_api_from_signature
from .treeleaf import iterate_tree, transform_tree

__all__ = ("job", "structure", "unstructure")


# Support for None and NoneType can be convenient
# See https://github.com/python-attrs/cattrs/issues/346
cattrs.register_structure_hook(NoneType, lambda d, t: None)
cattrs.register_unstructure_hook(NoneType, lambda d: None)


@attrs.define
class Job(MetaFuncBase):
    """A metafunction implementation of a job script.

    Attributes
    ----------
    template
        The absolute path to a template directory.
    jobinfo_source
        The source loaded from the ``jobinfo.py`` file.
    resources
        The resources defined in ``jobinfo.py``.
    parameters_api_func
        The parameters_api_func defined in ``jobinfo.py``.
    result_mock_func
        The result_mock_func defined in ``jobinfo.py``.
    """

    template: Path = attrs.field()
    jobinfo_source: str = attrs.field()
    resources: dict[str, Any] = attrs.field(init=False)
    can_resume: bool = attrs.field(init=False)
    parameters_func: Callable = attrs.field(init=False)
    mock_func: Callable = attrs.field(init=False)

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
        self.can_resume = ns.get("can_resume", False)
        self.parameters_func = ns.get("parameters")
        self.mock_func = ns["mock"]

    @classmethod
    def from_template(cls, template: str | Path) -> "Job":
        """Initialize a job script from a template directory.

        Parameters
        ----------
        template
            The template directory must contain a ``jobinfo.py`` file with the metadata
            needed to schedule the job in Parman.
            See module-level docstring for more information on this file.
            When the template is a relative path, it gets converted to an absolute one.
        """
        template = Path(template).absolute()
        with open(template / "jobinfo.py") as f:
            jobinfo_source = f.read()
        return cls(template, jobinfo_source)

    def describe(
        self,
        clerk: ClerkBase,
        locator: str | Path,
        script: str,
        kwargs: dict[str, Any],
        env: dict[str, str],
    ) -> Any:
        """Short descriptor of the job.

        See ``__call__`` method for parameter documentation.
        """
        return locator

    def __call__(
        self,
        clerk: ClerkBase,
        locator: str | Path,
        script: str,
        kwargs: dict[str, Any],
        env: dict[str, str],
    ) -> Any:
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
        env
            A dictionary with environment variables for the subprocess.

        Returns
        -------
        result
            The structured contents of the ``result.json`` file created by the job script.
            Output files needed by following jobs must be included here.
        """
        result_api = type_api_from_mock(self.mock_func(**kwargs))
        with clerk.workdir(locator) as workdir:
            path_kwargs = workdir / clerk.pull(locator / Path("kwargs.json"), locator, workdir)
            path_result = workdir / clerk.pull(locator / Path("result.json"), locator, workdir)
            todo_job = False

            # If kwargs present, we'll assume the job has been started already
            # (and possibly finished).
            expected_kwargs = clerk.localize(kwargs, locator, workdir)
            if path_kwargs.is_file():
                # If kwargs inconsistent -> refresh or raise exception
                with open(path_kwargs) as f:
                    found_kwargs = json.load(f)
                unstruct_kwargs = unstructure(expected_kwargs)
                if found_kwargs is None:
                    # The file kwargs.json contains "null".
                    # It is assumed that the old kwargs.json is manually flagged as outdated
                    # and safe to be refreshed.
                    print(f"Rewriting nullified kwargs.json in {locator}")
                    with open(workdir / "kwargs.json", "w") as f:
                        json.dump(unstruct_kwargs, f, indent=2)
                    clerk.push("kwargs.json", locator, workdir)
                elif found_kwargs != unstruct_kwargs:
                    with open(workdir / "kwargs-new.json", "w") as f:
                        json.dump(unstruct_kwargs, f, indent=2)
                    clerk.push("kwargs-new.json", locator, workdir)
                    raise ValueError(
                        f"Existing kwarg.json in {locator} inconsistent with new kwargs. "
                        "Added kwargs-new.json for comparison."
                    )
                if not path_result.exists() and self.can_resume:
                    todo_job = True
            else:
                # Check for the presence of a result.json file,
                # If present, this would suggest a broken state of the job
                if path_result.is_file():
                    raise ValueError(f"Found result.json in {locator} while kwargs.json is absent.")
                todo_job = True

            # If hashes file is present, even if empty, it should match the computed hashes.
            # If missing -> recreate.
            path_sha256 = workdir / clerk.pull(locator / Path("kwargs.sha256"), locator, workdir)
            expected_hashes = compute_hashes(expected_kwargs, workdir)
            if path_sha256.is_file():
                # If files in kwargs have changes hashes -> raise exception
                found_hashes = load_hashes(path_sha256)
                if found_hashes != expected_hashes:
                    dump_hashes(workdir / "kwargs-new.sha256", expected_hashes)
                    clerk.push("kwargs-new.sha256", locator, workdir)
                    raise ValueError(
                        f"Existing kwarg.json in {locator} inconsistent with new hashes. "
                        "Added kwargs-new.sha256 for comparison."
                    )
            else:
                dump_hashes(workdir / "kwargs.sha256", expected_hashes)
                clerk.push("kwargs.sha256", locator, workdir)

            if todo_job:
                if self.can_resume:
                    print(f"Starting or resuming {locator}")
                else:
                    print(f"Starting {locator}")

                # Define useful environment variable
                parman_env = env | {"PARMAN_WORKDIR": os.getcwd()}
                write_sh_env(workdir / "jobenv.sh", parman_env)

                # Initialize the work directory
                shutil.copytree(self.template, workdir, dirs_exist_ok=True)
                local_kwargs = clerk.localize(kwargs, locator, workdir)
                with open(workdir / "kwargs.json", "w") as f:
                    json.dump(unstructure(local_kwargs), f, indent=2)
                dump_hashes(workdir / "kwargs.sha256", compute_hashes(local_kwargs, workdir))

                # Run the job
                fn_out = workdir / f"{script}.out"
                fn_err = workdir / f"{script}.err"
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
                            env=os.environ | parman_env,
                        )
                except subprocess.CalledProcessError as exc:
                    if fn_err.is_file():
                        with open(fn_err) as f:
                            sys.stderr.write(f.read())
                    raise RuntimeError(f"Script {locator} failed: {script}.") from exc

                # When we got here, the job ran without raising an exception.
                clerk.push("kwargs.json", locator, workdir)
                clerk.push("kwargs.sha256", locator, workdir)
                clerk.push(script, locator, workdir)
                clerk.push(f"{script}.out", locator, workdir)
                clerk.push(f"{script}.err", locator, workdir)

                # There may be some extra files, not explicitly included in the results,
                # worth keeping.
                clerk.push("jobinfo.py", locator, workdir)
                fn_extra = workdir / "result.extra"
                if fn_extra.is_file():
                    with open(fn_extra) as f:
                        for line in f:
                            line = strip_line(line)
                            if len(line) > 0:
                                clerk.push(line.strip(), locator, workdir)
                    clerk.push("result.extra", locator, workdir)
                print(f"Completed {locator}")
            else:
                print(f"Not rerunning {locator}")

            if path_result.exists():
                clerk.push("result.json", locator, workdir)
                with open(path_result) as f:
                    result_local = structure("result", json.load(f), result_api)
                    result = clerk.globalize(result_local, locator, workdir)
            else:
                raise OSError(f"No result.json after completion of {locator}")

        return result

    def get_parameters_api(
        self,
        clerk: ClerkBase,
        locator: str | Path,
        script: str,
        kwargs: dict[str, Any],
        env: dict[str, str],
    ) -> dict[str, Any]:
        """Return the parameter API derived from the ``jobinfo.py`` metadata.

        See ``__call__`` method for parameter documentation.
        """
        parameters_api = MetaFuncBase.get_parameters_api(self)
        if self.parameters_func is None:
            parameters_api["kwargs"] = type_api_from_signature(inspect.signature(self.mock_func))
        else:
            parameters_api["kwargs"] = self.parameters_func(**kwargs)
        return parameters_api

    def get_result_mock(
        self,
        clerk: ClerkBase,
        locator: str | Path,
        script: str,
        kwargs: dict[str, Any],
        env: dict[str, str],
    ) -> Any:
        """Return a mock result, derived from the ``jobinfo.py`` metadata.

        See ``__call__`` method for parameter documentation.
        """
        return self.mock_func(**kwargs)

    def get_resources(
        self,
        clerk: ClerkBase,
        locator: str | Path,
        script: str,
        kwargs: dict[str, Any],
        env: dict[str, str],
    ) -> dict:
        """Return the value of the resources dictionary in ``jobinfo.py``

        See ``__call__`` method for parameter documentation.
        """
        return self.resources

    def get_defaults(self):
        """Get the default keyword arguments specified in the jobinfo file."""
        signature = inspect.signature(self.mock_func)
        defaults = {}
        for name, parameter in signature.parameters.items():
            default = parameter.default
            if default != parameter.empty:
                defaults[name] = default
        return defaults


def structure(prefix: str, json_data: Any, data_api: Any) -> Any:
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
        """Convert an unstructured leaf to a structured one."""
        if not isinstance(leaf_api, type | types.GenericAlias):
            raise TypeError(
                f"{prefix} at {mulidx}: cannot structure type {leaf_api}, leaf = {json_leaf}"
            )
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


def unstructure(data: Any) -> Any:
    """Unstructure structured data.

    This is the inverse of ``structure`` and returns a JSON-able result.
    """
    return transform_tree(lambda _, leaf: cattrs.unstructure(leaf), data)


def compute_hashes(data: Any, workdir: Path) -> dict[str, str]:
    """Compute SHA256 hashes of all files present in data (nested in lists and dictionaries)."""
    result = {}
    for _, leaf in iterate_tree(data):
        if isinstance(leaf, Path):
            sha = hashlib.sha256()
            with open(workdir / leaf, "rb") as f:
                while True:
                    block = f.read(1048576)
                    if len(block) == 0:
                        break
                    sha.update(block)
            result[str(leaf)] = sha.hexdigest()
    return result


def dump_hashes(path_sha256: Path, hashes: dict[str, str]):
    """Write path hashes to file, compatible with sha256sum."""
    with open(path_sha256, "w") as f:
        for path, sha in sorted(hashes.items()):
            f.write(f"{sha}  {path}\n")


def load_hashes(path_sha256: Path) -> dict[str, str]:
    """Load path hashes from file, compatible with sha256sum."""
    result = {}
    with open(path_sha256) as f:
        for line in f:
            sha = line[:64].lower()
            path = line[66:].strip()
            if len(path) == 0 or len(sha) != 64 or any(c not in string.hexdigits for c in sha):
                raise ValueError(f"Incorrectly formatted checksum line:\n{line[:-1]}")
            result[path] = sha
    return result


def strip_line(line: str):
    """Strip comment from line and strip whitespace."""
    comment_pos = line.find("#")
    if comment_pos >= 0:
        line = line[:comment_pos]
    return line.strip()


def write_sh_env(path_rc: str, env: dict[str, str]):
    """Write a resource configuration file to simulate the environment in which the job runs.

    The resulting file can be sourced in a bash shell.

    Parameters
    ----------
    path_rc
        The file to be written.
    env
        A dictionary with environment variables to include.
    """
    with open(path_rc, "w") as f:
        for key, value in env.items():
            if not (isinstance(key, str) and re.match("[_a-zA-Z][_a-zA-Z0-9]*", key)):
                raise ValueError(f"Invalid shell variable name: {key}")
            f.write(f'export {key}="{value}"\n')


@attrs.define
class JobFactory:
    """Convenience class for instantiating new jobs"""

    clerk: ClerkBase = attrs.field(default=attrs.Factory(LocalClerk))
    script: str = attrs.field(default="run")
    env: dict[str, str] = attrs.field(default=attrs.Factory(dict))
    _cache: dict[str, Job] = attrs.field(init=False, default=attrs.Factory(dict))

    def __call__(self, template: str, locator: str, **kwargs) -> Closure:
        """Create a new job with the locator and keyword arguments."""
        job_obj = self._cache.get(template)
        if job_obj is None:
            job_obj = Job.from_template(template)
            self._cache[template] = job_obj
        all_kwargs = job_obj.get_defaults()
        all_kwargs.update(kwargs)
        return Closure(job_obj, [self.clerk, locator, self.script, all_kwargs, self.env])


job = JobFactory()
