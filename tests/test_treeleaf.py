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
"""Unit tests for parman.treeleaf."""

import pytest
from parman.treeleaf import get_tree, iterate_tree, same, transform_tree


@pytest.mark.parametrize(
    ("tree", "mulidx", "value"),
    [
        ("aaa", (), "aaa"),
        (1, (), 1),
        ([], (), []),
        ({}, (), {}),
        ([1, 2], (0,), 1),
        ([1, 2, 5], (-1,), 5),
        ({1: "a", "b": 2}, (1,), "a"),
        ({1: "a", "b": 2}, ("b",), 2),
        ({1: "a", "b": [2, 3]}, ("b", 1), 3),
        ({1: "a", "b": [2, [{"c": 3}]]}, ("b", 1, 0, "c"), 3),
    ],
)
def test_get_tree(tree, mulidx, value):
    assert get_tree(tree, mulidx) == value


@pytest.mark.parametrize(
    ("tree", "items"),
    [
        ("aaa", [((), "aaa")]),
        ([3, "b"], [((0,), 3), ((1,), "b")]),
        ([3, [["b"]]], [((0,), 3), ((1, 0, 0), "b")]),
        ({2: "a", "b": 3}, [((2,), "a"), (("b",), 3)]),
        ({2: "a", "b": [3, 4]}, [((2,), "a"), (("b", 0), 3), (("b", 1), 4)]),
        ({2: "a", "b": [[[3]], 4]}, [((2,), "a"), (("b", 0, 0, 0), 3), (("b", 1), 4)]),
    ],
)
def test_iterate_tree_single(tree, items):
    assert list(iterate_tree(tree)) == items


@pytest.mark.parametrize(
    ("trees", "items"),
    [
        (["aaa", "bbb", [4, 3]], [((), ("aaa", "bbb", [4, 3]))]),
        ([[2, 1], [4, 3]], [((0,), (2, 4)), ((1,), (1, 3))]),
        (
            [[1, {"a": 2, "c": 3}], [4, {"a": 5, "c": 6}]],
            [
                ((0,), (1, 4)),
                ((1, "a"), (2, 5)),
                ((1, "c"), (3, 6)),
            ],
        ),
        (
            [[1, {"a": 2, "c": 3}], [4, {"a": 5, "b": 6}]],
            [
                ((0,), (1, 4)),
                ((1,), ({"a": 2, "c": 3}, {"a": 5, "b": 6})),
            ],
        ),
        (
            [{"s": 25, "e": ["a", "b"]}, {"e": list[str], "s": int}],
            [
                (("s",), (25, int)),
                (("e",), (["a", "b"], list[str])),
            ],
        ),
    ],
)
def test_iterate_tree_multiple(trees, items):
    assert list(iterate_tree(*trees)) == items


@pytest.mark.parametrize(
    ("intrees", "outtree"),
    [
        ([1], "()1"),
        ([1, 2], "()12"),
        ([3.0], "()3.0"),
        ([[]], []),
        ([[1, 2], [3, 4]], ["(0,)13", "(1,)24"]),
        ([{"a": 1, "b": 2}, {"a": 3, "b": 4}], {"a": "('a',)13", "b": "('b',)24"}),
        ([[1, 2], 3], "()[1, 2]3"),
        (
            [[1, {"a": 2, "c": 3}], [4, {"a": "x", "c": "y"}]],
            ["(0,)14", {"a": "(1, 'a')2x", "c": "(1, 'c')3y"}],
        ),
    ],
)
def test_transform_tree_str(intrees, outtree):
    def transform_str(mulidx, *leafs):
        return str(mulidx) + "".join(str(leaf) for leaf in leafs)

    assert transform_tree(transform_str, *intrees) == outtree


def test_same():
    assert same([1, 1, 1])
    assert not same([1, 1, 2])
    assert same((1, 1, 1))
    assert not same((1, 1, 2))
    assert same(item for item in [1, 1, 1])
    assert not same(item for item in [1, 1, 2])
    with pytest.raises(ValueError):
        same([])
    with pytest.raises(ValueError):
        same(())
    with pytest.raises(ValueError):
        same(item for item in [])
    assert same([1])
    assert same((1,))
    assert same(item for item in [1])
