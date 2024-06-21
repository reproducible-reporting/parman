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
"""Recursive processing of hierarchical trees of lists and dictionaries.

The functions below facilitate operations on nested lists + dictionaries + other types.

Terminology
-----------

A set of nested lists and dictionaries is called a ``tree``.

The name ``leaf`` is used for a list item or dictionary value in a ``tree``
that is itself not a list or dictionary.
The functions below treat leafs (including tuples) as opaque objects that cannot
be recursed into.
"""

from collections.abc import Callable, Generator, Iterable
from typing import Any

__all__ = ("get_tree", "iterate_tree", "transform_tree", "same")


def get_tree(tree: Any, mulidx: tuple) -> Any:
    """Get an leaf (or subtree) from a tree.

    Parameters
    ----------
    tree
        The tree to get the leaf or subtree from.
    mulidx
        A tuple of indexes, starting with the most toplevel list or dictionary.

    Returns
    -------
    element
        The leaf or subtree corresponding to mulidx.
    """
    if len(mulidx) == 0:
        return tree
    if len(mulidx) > 0:
        return get_tree(tree[mulidx[0]], mulidx[1:])
    return None


def same(items: Iterable) -> bool:
    """Return True only if all items are equal."""
    iitems = iter(items)
    try:
        first = next(iitems)
    except StopIteration as exc:
        raise ValueError("Iterator passed to same function has no items.") from exc
    return all(first == item for item in iitems)


def iterate_tree(*trees: Any) -> Generator[tuple[tuple, Any], None, None]:
    """Iterate over the (corresponding) leafs of one or more trees.

    Parameters
    ----------
    trees
        One or more trees with compatible structure.
        Recursion into subtrees and leafs is continued for as long as
        the recursion makes sense in all given trees.
        If an inconsistency is encountered, the incompatible lists or dicts are treated
        as leafs instead.

    Yields
    ------
    leafs
        When one tree is given, a sinlge leaf is yielded at a time.
        When multiple trees are given, a tuple of corresponding leafs is yielded.
    """
    handled = False
    if same(type(tree) for tree in trees):
        if isinstance(trees[0], list) and same(len(tree) for tree in trees):
            for intidx, items in enumerate(zip(*trees, strict=True)):
                for subidx, subtrees in iterate_tree(*items):
                    yield (intidx, *subidx), subtrees
            handled = True
        elif isinstance(trees[0], dict) and same(set(tree.keys()) for tree in trees):
            for keyidx in trees[0]:
                items = [tree[keyidx] for tree in trees]
                for subidx, subtrees in iterate_tree(*items):
                    yield (keyidx, *subidx), subtrees
            handled = True
    if not handled:
        if len(trees) == 1:
            yield (), trees[0]
        else:
            yield (), trees


def transform_tree(transform: Callable, *trees: Any, _mulidx: tuple = ()) -> Any:
    """Transform one or more trees elementwise into a new one.

    Parameters
    ----------
    transform
        A callable, taking a number of arguments equal to the number of trees plus one.
        The first argument is the mulidx of the current position on the tree(s).
        The remaining arguments are corresponding leaf at that mulidx, one from each input tree.
        The return value is the leaf on the resulting tree.
    trees
        One or more input trees.
    _mulidx
        Users should not provide anything else than the default value.
        This is the prefix of the mulidx, which is extended as it is passed into
        recursive calls.

    Returns
    -------
    outtree
        The result of the transformation:
        a new tree whose leaf equal the return values of the transform function calls.
    """
    if same(type(tree) for tree in trees):
        if isinstance(trees[0], list) and same(len(tree) for tree in trees):
            result = []
            for intidx, items in enumerate(zip(*trees, strict=True)):
                result.append(transform_tree(transform, *items, _mulidx=(*_mulidx, intidx)))
            return result
        if isinstance(trees[0], dict) and same(set(tree) for tree in trees):
            result = {}
            for keyidx in trees[0]:
                items = [tree[keyidx] for tree in trees]
                result[keyidx] = transform_tree(transform, *items, _mulidx=(*_mulidx, keyidx))
            return result
    return transform(_mulidx, *trees)
