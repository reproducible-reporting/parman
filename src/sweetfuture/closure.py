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
"""Bundled information for a function to be submitted."""

import inspect
from typing import Any

import attrs

from .metafunc import MetaFuncBase, validate


@attrs.define
class Closure:
    """A meta function and concrete arguments to be passed in."""

    metafunc: MetaFuncBase = attrs.field()
    args: list = attrs.field(default=attrs.Factory(list))
    kwargs: dict[str, Any] = attrs.field(default=attrs.Factory(dict))

    def validated_call(self) -> Any:
        parameters = self.get_parameters()
        parameters_api = self.get_parameter_api()
        validate("parameters", parameters, parameters_api)
        result = self.metafunc(*self.args, **self.kwargs)
        result_api = self.get_result_api()
        validate("result", result, result_api)
        return result

    def cached_result(self) -> Any:
        return self.metafunc.cached_result(*self.args, **self.kwargs)

    def get_parameters(self) -> dict[str, Any]:
        """Return a dictionary with all parameters (including positional ones)."""
        signature = inspect.signature(self.metafunc.__call__)
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
        return self.metafunc.get_result_mock(*self.args, **self.kwargs)

    def get_result_api(self) -> Any:
        return self.metafunc.get_result_api(*self.args, **self.kwargs)

    def get_resources(self) -> dict:
        return self.metafunc.get_resources(*self.args, **self.kwargs)