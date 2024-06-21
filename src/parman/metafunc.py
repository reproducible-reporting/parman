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
from collections.abc import Callable
from typing import Any

import attrs
import cattrs

from .treeleaf import iterate_tree, transform_tree

__all__ = (
    "MetaFuncBase",
    "validate",
    "type_api_from_signature",
    "type_api_from_mock",
    "MinimalMetaFunc",
)


@attrs.define
class MetaFuncBase:
    """A callable and its metadata for use in a parallel workflow.

    Subclasses override the following methods:

    - ``describe`` (optional, highly recommended)
    - ``__call__`` (mandatory)
    - ``get_signature`` (optional)
    - ``get_parameters_api`` (optional)
      The default behavior is to deduce the signature from the __call__ method,
      but this may be modified in subclasses.
    - ``get_result_api`` (mandatory)
    - ``get_resources`` (optional)
      The default is to return an empty dictionary.

    All these methods must take the same arguments.
    Here, they take generic (*args, **kwargs), but subclasses can change this.
    """

    def describe(self, *args, **kwargs) -> str:
        """Describe this metafunc."""
        return "unnamed"

    def __call__(self, *args, **kwargs) -> Any:
        """The method to be submitted to an executor."""
        raise NotImplementedError

    def get_signature(self):
        """Return the `inspect.Signature` object of the callable."""
        return inspect.signature(self.__call__)

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
        return type_api_from_signature(self.get_signature())

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
        return {}


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
    for mulidx, (leaf, leaf_type) in iterate_tree(data, type_api):
        if isinstance(leaf_type, types.GenericAlias):
            # Use cattrs magic to check the type.
            # The GenericAlias types cannot be checked with isinstance.
            try:
                cattrs.structure(leaf, leaf_type)
            except cattrs.IterableValidationError as exc:
                raise TypeError(
                    f"{prefix} at {mulidx}: {leaf} does not conform {leaf_type}"
                ) from exc
            except cattrs.StructureHandlerNotFoundError as exc:
                raise TypeError(
                    f"{prefix} at {mulidx}: type {leaf_type} cannot be instantiated"
                ) from exc
        else:
            # Standard Python type check
            try:
                if not isinstance(leaf, leaf_type):
                    raise TypeError(f"{prefix} at {mulidx} is not of type {leaf_type}")
            except Exception as exc:
                raise TypeError(
                    f"{prefix} at {mulidx}: cannot type-check {leaf} with {leaf_type}"
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

    def transform(_, mock_leaf):
        """Get the type of a leaf, raise error if it is already a type."""
        if isinstance(mock_leaf, type):
            raise TypeError("A mock result cannot contain types.")
        return type(mock_leaf)

    return transform_tree(transform, mock_api)


@attrs.define
class MinimalMetaFunc(MetaFuncBase):
    """A bare-bones implementation of MetaFuncBase without caching."""

    function: Callable = attrs.field()
    mock: Callable = attrs.field(default=None)

    def describe(self, *args, **kwargs) -> str:
        """Describe this metafunc."""
        return self.function.__name__

    def __call__(self, *args, **kwargs) -> Any:
        """The method to be submitted to an executor."""
        return self.function(*args, **kwargs)

    def get_signature(self) -> inspect.Signature:
        """Return a signature of __call__, used for type checking."""
        return inspect.signature(self.function)

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
        if self.mock is None:
            return object()
        return self.mock(*args, **kwargs)
