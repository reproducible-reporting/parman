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


import importlib.util
import os
import types
from pathlib import Path

import attrs
import cattrs

from ..recursive import recursive_iterate, recursive_transform

__all__ = ("JobInfo", "validate", "make_example", "structure", "unstructure")


# The following can be removed once cattrs 23 is released:
# See https://github.com/python-attrs/cattrs/issues/81
cattrs.register_structure_hook(Path, lambda d, t: Path(d))
cattrs.register_unstructure_hook(Path, lambda d: str(d))


@attrs.define
class JobInfo:
    """Job information.

    Note that instances of this class cannot be pickled.
    They are designed to live in the main thread only.
    """

    get_kwargs_api: callable = attrs.field()
    get_result_api: callable = attrs.field()
    resources: dict = attrs.field(default=attrs.Factory(dict))

    @classmethod
    def from_template(cls, template):
        return cls.from_file(os.path.join(template, "jobinfo.py"))

    @classmethod
    def from_file(cls, path_jobapi):
        assert os.path.isfile(path_jobapi)
        spec = importlib.util.spec_from_file_location("<jobinfo>", path_jobapi)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        resources = getattr(module, "resources", {})
        return cls(module.get_kwargs_api, module.get_result_api, resources)

    def get_apis(self, kwargs):
        return self.get_kwargs_api(), self.get_result_api(kwargs)


def validate(prefix, data, data_api):
    for mulidx, (field, field_api) in recursive_iterate(data, data_api):
        # Use cattrs magic to check the type
        if not isinstance(field_api, (type, types.GenericAlias)):
            raise TypeError(f"{prefix} at {mulidx}: cannot type-check {field} with {field_api}")
        try:
            cattrs.structure(field, field_api)
        except cattrs.IterableValidationError:
            raise TypeError(f"{prefix} at {mulidx}: {field} does not conform {field_api}")
        except cattrs.StructureHandlerNotFoundError:
            raise TypeError(f"{prefix} at {mulidx}: type {field_api} cannot be instantiated")


FIELD_EXAMPLES = {
    int: 1,
    float: 1.0,
    str: "foo",
    Path: Path("__example_file__.txt"),
}


def make_example(data_api):
    def transform(mulidx, field_api):
        example = FIELD_EXAMPLES.get(field_api)
        if example is None:
            example_fn = getattr(field_api, "_make_example", None)
            if example_fn is None:
                raise TypeError(f"At {mulidx}: cannot generate an example of {field_api}")
            example = example_fn()
        return example

    return recursive_transform(transform, data_api)


def structure(prefix, json_data, data_api):
    def transform(mulidx, json_field, field_api):
        if not isinstance(field_api, (type, types.GenericAlias)):
            raise TypeError(f"{prefix} at {mulidx}: cannot structure type {field_api}")
        try:
            return cattrs.structure(json_field, field_api)
        except cattrs.IterableValidationError:
            raise TypeError(f"{prefix} at {mulidx}: {json_field} does not conform {field_api}")
        except cattrs.StructureHandlerNotFoundError:
            raise TypeError(f"{prefix} at {mulidx}: type {field_api} cannot be instantiated")

    return recursive_transform(transform, json_data, data_api)


def unstructure(data):
    return recursive_transform(lambda _, field: cattrs.unstructure(field), data)
