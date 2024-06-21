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
"""Unit tests for parman.metafunc."""

import inspect

from parman.metafunc import MinimalMetaFunc


def compute_sum(first: int, second: int = 2) -> int:
    return first + second


def compute_sum_mock(first: int, second: int = 2) -> int:
    return 42


def test_basics():
    metafunc = MinimalMetaFunc(compute_sum, compute_sum_mock)
    assert metafunc.describe() == "compute_sum"
    assert metafunc(1) == 3
    assert metafunc.get_signature() == inspect.signature(compute_sum)
    assert metafunc.get_parameters_api() == {"first": int, "second": int}
    assert metafunc.get_result_mock(1, 3) == 42
    assert metafunc.get_result_api(1, 3) == int
    assert metafunc.get_resources() == {}
