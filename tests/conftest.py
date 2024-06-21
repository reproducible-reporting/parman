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
"""Shared fixtures for the unit tests."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest


@pytest.fixture(params=[ProcessPoolExecutor, ThreadPoolExecutor])
def pool(request):
    """Default executor from concurrent.futures."""
    pool_executor_class = request.param
    with pool_executor_class() as pool:
        yield pool


@pytest.fixture(params=[ProcessPoolExecutor, ThreadPoolExecutor])
def pool1(request):
    """Single-worker executor from concurrent.futures."""
    pool_executor_class = request.param
    with pool_executor_class(max_workers=1) as pool:
        yield pool
