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
"""Function definition with metadata needed to setup the workflow.

The MetaFuncBase class below defines two API methods:

- `get_parameter_api` returns an `type_api` dictionary describing the parameters.
- `get_result_api` returns an `mock_api` describing the return value.

These APIs provide all prior knowledge on the ins and outs of a callable in a workflow.
The more detailed these APIs, the more useful they become.

Three kinds of objects can be used as APIs:

- For function parameters: an annotation or type, used to type check function parameters.
- For a result: an mock example, from which the type can be inferred.
  This is used for type checking and to create mock examples in the dry run.
- As a special case, a list or a dictionary, whose items or values can be any of the previous two,
  depending on the use case (parameters or results).
  This allows for a hierarchical data structure in the API.

By default, the parameter API is inferred from the __call__ signature,
but this may be overriden for more advanced type checks.
"""

import inspect
import types
from typing import Any

import attrs
import cattrs

from .recursive import recursive_iterate, recursive_transform

__all__ = ("MetaFuncBase", "validate", "type_api_from_signature", "type_api_from_mock")


@attrs.define
class MetaFuncBase:
    """A callable and its metadata for use in a parallel workflow.

    Subclasses implement `__call__`, `get_parameters_api`, `get_result_api` and `get_resources`
    such that they all take the same arguments.
    Here, methodes take generic (*args, **kwargs), but subclasses can change this.

    The default behavior is to deduce the signature from the __call__ method,
    but this may be modified in subclasses.
    """

    def __call__(self, *args, **kwargs) -> Any:
        """The method to be submitted to an executor."""
        raise NotImplementedError

    def cached_result(self, *args, **kwargs) -> Any:
        """Return a cached result in case recomputation can be avoided.

        This should give the same result as __call__, but fast enough for the main process.
        When no cached result is available, NotImplemented is returned.
        """
        return NotImplemented

    def get_parameters_api(self, *args, **kwargs) -> dict[str, Any]:
        """A method returning API details for function parameters.

        The default behavior is to deduce this API from the signature.
        From that signature, only the parameters are used, not the return annotation.
        Annotations take precedence over default values to infer the type

        Returns
        -------
        type_api
            A dictionary mapping each argument name to an API specification.
            See module docstring for details on the API spec.
        """
        return type_api_from_signature(inspect.signature(self.__call__))

    def get_result_mock(self, *args, **kwargs) -> Any:
        """A method returning API of the result, in the form of a mock.

        There is no default behavior to derive this from the __call__ signature,
        because the return value must be a mock example consistent with the parameters.

        Returns
        -------
        result_api
            An API specification for the result.
            See module docstring for details on the API spec.
        """
        raise NotImplementedError

    def get_result_api(self, *args, **kwargs) -> Any:
        """A method returning a type checking API of the result."""
        return type_api_from_mock(self.get_result_mock(*args, **kwargs))

    def get_resources(self, *args, **kwargs) -> dict:
        """A method returning a dictionary with resource information.

        Returns
        -------
        resources
            A dictionary with information that Runners may use decide how and where to
            execute this function.
        """
        raise NotImplementedError


def validate(prefix, data, type_api):
    """Recursive type checking of values against an API.

    Parameters
    ----------
    prefix
        A prefix to be used in error messages.
    data
        The data to be type checked.
    type_api
        The API to use for checking, see module docstring for details.

    Raises
    ------
    TypeError
        When a type error is encountered in the data.
    """
    for mulidx, (field, field_type) in recursive_iterate(data, type_api):
        # Use cattrs magic to check the type
        if not isinstance(field_type, (type, types.GenericAlias)):
            raise TypeError(f"{prefix} at {mulidx}: cannot type-check {field} with {field_type}")
        try:
            cattrs.structure(field, field_type)
        except cattrs.IterableValidationError as exc:
            raise TypeError(f"{prefix} at {mulidx}: {field} does not conform {field_type}") from exc
        except cattrs.StructureHandlerNotFoundError as exc:
            raise TypeError(
                f"{prefix} at {mulidx}: type {field_type} cannot be instantiated"
            ) from exc


def type_api_from_signature(signature):
    """Construct a type api from a function signature (inspect module)."""
    result = {}
    for name, parameter in signature.parameters.items():
        if parameter.annotation != parameter.empty:
            result[name] = parameter.annotation
        elif parameter.default != parameter.empty:
            result[name] = type(parameter.default)
        else:
            raise TypeError(
                f"Type of parameter {name} cannot be inferred. "
                "You need to add an annotation or default value."
            )
    return result


def type_api_from_mock(mock_api):
    """Derive a type_api (suitable for the validate function) from example data, mock_api."""

    def transform(_, mock_field):
        if isinstance(mock_field, type):
            raise TypeError("A mock_api cannot contain types.")
        return type(mock_field)

    return recursive_transform(transform, mock_api)