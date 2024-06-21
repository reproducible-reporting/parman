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
"""A simple wrapper for convenient parallelization of serial scripts."""

from functools import wraps

from .closure import Closure
from .metafunc import MinimalMetaFunc

__all__ = ("wrap",)


def wrap(runner, function, mock=None):
    """Create a wrapper that will submit function calls to a Runner instance transparently."""
    metafunc = MinimalMetaFunc(function, mock)

    @wraps(function)
    def wrapper(*args, **kwargs):
        closure = Closure(metafunc, list(args), kwargs)
        return runner(closure)

    return wrapper
