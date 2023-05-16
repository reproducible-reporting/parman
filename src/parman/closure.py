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
"""Bundled information for a function to be submitted."""

from concurrent.futures import Future
from copy import deepcopy
from typing import Any

import attrs

from .metafunc import MetaFuncBase, validate
from .treeleaf import transform_tree


@attrs.define
class Closure:
    """A meta function and concrete arguments to be passed in."""

    metafunc: MetaFuncBase = attrs.field()
    args: list = attrs.field(default=attrs.Factory(list))
    kwargs: dict[str, Any] = attrs.field(default=attrs.Factory(dict))

    def __attrs_post_init__(self):
        # Take copies of the arguments at the time the closure is created,
        # so that later in-place changes to the original arguments are ignored.
        # This eliminates race conditions that would otherwise by hard to
        # detect with the ThreadPoolExecutor (and possibly others).
        self.args = _safe_deepcopy_data(self.args)
        self.kwargs = _safe_deepcopy_data(self.kwargs)

    def describe(self) -> str:
        """Describe this closure."""
        return self.metafunc.describe(*self.args, **self.kwargs)

    def validated_call(self) -> Any:
        """Validate the parameters, call the metafunction, validate and return the result."""
        parameters = self.get_parameters()
        parameters_api = self.get_parameter_api()
        validate("parameters", parameters, parameters_api)
        result = self.metafunc(*self.args, **self.kwargs)
        result_api = self.get_result_api()
        validate("result", result, result_api)
        return result

    def get_parameters(self) -> dict[str, Any]:
        """Return a dictionary with all parameters (including positional ones)."""
        signature = self.metafunc.get_signature()
        bound_arguments = signature.bind(*self.args, **self.kwargs)
        bound_arguments.apply_defaults()
        return dict(bound_arguments.arguments)

    def get_parameter_api(self) -> dict[str, Any]:
        """Return a type checking API for the parameters."""
        return self.metafunc.get_parameters_api(*self.args, **self.kwargs)

    def validate_parameters(self):
        """Validate and return the parameters"""
        parameters = self.get_parameters()
        parameters_api = self.get_parameter_api()
        validate("parameters", parameters, parameters_api)

    def get_result_mock(self) -> Any:
        """Get the mock result."""
        return self.metafunc.get_result_mock(*self.args, **self.kwargs)

    def get_result_api(self) -> Any:
        """Get the result API, typically derived from the mock result."""
        return self.metafunc.get_result_api(*self.args, **self.kwargs)

    def get_resources(self) -> dict:
        """Get the resources dictionary."""
        return self.metafunc.get_resources(*self.args, **self.kwargs)


def _safe_deepcopy_data(data: Any) -> Any:
    """Return a deepcopy, except that futures are passed through.

    Futures are only handled correctly when they are list items or dictionary values,
    and only when these lists or dictionaries are list items or dictionary values themselves,
    recursively all the way up to the data argument.
    In all other cases, the deepcopy of the Future instance may result in an error.
    (Circumventing that error, e.g. with __deepcopy__, will give unpredictable results.)
    """
    return transform_tree(
        lambda _, leaf: leaf if isinstance(leaf, Future) else deepcopy(leaf), data
    )
