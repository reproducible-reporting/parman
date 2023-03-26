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
"""Recursive processing of hierarchical unstructured data.

The functions below facilitate operations on nested lists + dicts + other types.
"""


__all__ = ("recursive_get", "recursive_set", "recursive_iterate", "recursive_transform", "same")


def recursive_get(tree, mulidx: tuple):
    if len(mulidx) == 0:
        return tree
    elif len(mulidx) > 0:
        return recursive_get(tree[mulidx[0]], mulidx[1:])


def recursive_set(tree, mulidx: tuple, value):
    # TODO: REMOVE THIS, too fragile and not needed anymore.
    if len(mulidx) == 0:
        return value
    idx = mulidx[0]
    subidx = mulidx[1:]
    if isinstance(idx, int):
        if idx < 0:
            raise ValueError("Negative list indexes are not supported in recursive_set")
        if tree is None:
            tree = [None] * (idx + 1)
        else:
            assert isinstance(tree, list)
            if len(tree) <= idx:
                tree.extend([None] * (len(tree) - idx + 1))
    else:
        if tree is None:
            tree = {}
        else:
            assert isinstance(tree, dict)
    tree[idx] = recursive_set(tree[idx], subidx, value)
    return tree


def same(items):
    iitems = iter(items)
    first = next(iitems)
    return all(first == item for item in iitems)


def recursive_iterate(*trees):
    handled = False
    if same(type(tree) for tree in trees):
        if isinstance(trees[0], list) and same(len(tree) for tree in trees):
            for intidx, items in enumerate(zip(*trees)):
                for subidx, subtrees in recursive_iterate(*items):
                    yield (intidx,) + subidx, subtrees
            handled = True
        elif isinstance(trees[0], dict) and same(set(tree.keys()) for tree in trees):
            for keyidx in trees[0].keys():
                items = [tree[keyidx] for tree in trees]
                for subidx, subtrees in recursive_iterate(*items):
                    yield (keyidx,) + subidx, subtrees
            handled = True
    if not handled:
        if len(trees) == 1:
            yield (), trees[0]
        else:
            yield (), trees


def recursive_transform(transform, *trees, mulidx=()):
    if same(type(tree) for tree in trees):
        if isinstance(trees[0], list) and same(len(tree) for tree in trees):
            result = []
            for intidx, items in enumerate(zip(*trees)):
                result.append(recursive_transform(transform, *items, mulidx=mulidx + (intidx,)))
            return result
        elif isinstance(trees[0], dict) and same(set(tree.keys()) for tree in trees):
            result = {}
            for keyidx in trees[0].keys():
                items = [tree[keyidx] for tree in trees]
                result[keyidx] = recursive_transform(transform, *items, mulidx=mulidx + (keyidx,))
            return result
    return transform(mulidx, *trees)
