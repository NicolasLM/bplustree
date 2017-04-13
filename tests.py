import itertools
import mmap

import pytest

from bplustree.const import ENDIAN
from bplustree.entry import Record, Reference
from bplustree.node import (Node, LonelyRootNode, RootNode, InternalNode,
                            LeafNode)
from bplustree.tree import BPlusTree
from bplustree.utils import pairwise


def test_create_in_memory():
    b = BPlusTree()
    assert b._fd is None
    b.close()


def test_write_read_page():
    b = BPlusTree()
    expected = b'abcdefgh' * int(mmap.PAGESIZE / 8)
    b._write_page(1, expected)
    assert b._read_page(1) == expected
    b.close()


def test_partial_node():
    b = BPlusTree()
    node = b.RootNode()
    assert node._page_size == b._page_size
    assert node._order == b._order
    b.close()


def test_insert_get_record_in_tree():
    b = BPlusTree()
    b.insert(1, 1024)
    assert b.get(1) == 1024
    b.close()

iterators = [
    range(0, 1000, 1),
    range(1000, 0, -1),
    list(range(0, 1000, 2)) + list(range(1, 1000, 2))
]
orders = [3, 4, 5, 6, 100]
matrix = itertools.product(iterators, orders)


@pytest.mark.parametrize('iterator,order', matrix)
def test_insert_split_in_tree(iterator, order):
    check_after_each_insert = False
    inserted = set()
    b = BPlusTree(order=order)

    for i in iterator:
        b.insert(i, i)
        inserted.add(i)

        if check_after_each_insert:
            for x in inserted:
                assert b.get(x) == x

    if not check_after_each_insert:
        for x in inserted:
            assert b.get(x) == x

    b.close()


@pytest.mark.parametrize('klass,order,min_children,max_children', [
    (LonelyRootNode, 7, 0, 6),
    (LonelyRootNode, 100, 0, 99),
    (RootNode, 7, 2, 7),
    (RootNode, 100, 2, 100),
    (InternalNode, 7, 4, 7),
    (InternalNode, 100, 50, 100),
    (LeafNode, 7, 3, 6),
    (LeafNode, 100, 49, 99),
])
def test_node_limit_children(klass, order, min_children, max_children):
    node = klass(mmap.PAGESIZE, order)
    assert node._min_children == min_children
    assert node._max_children == max_children


def test_get_node_from_page_data():
    data = (2).to_bytes(1, ENDIAN) + bytes(mmap.PAGESIZE - 1)
    assert isinstance(
        Node.from_page_data(mmap.PAGESIZE, 4, data=data),
        RootNode
    )


def test_record_serialization():
    r1 = Record(42, 8080)
    data = r1.dump()

    r2 = Record(data=data)
    assert r1 == r2
    assert r1.value == r2.value


def test_reference_serialization():
    r1 = Reference(42, 1, 2)
    data = r1.dump()

    r2 = Reference(data=data)
    assert r1 == r2
    assert r1.before == r2.before
    assert r1.after == r2.after


@pytest.mark.parametrize('klass', [
    LeafNode, InternalNode, RootNode, LonelyRootNode,
])
def test_empty_node_serialization(klass):
    n1 = klass(mmap.PAGESIZE, 7)
    data = n1.dump()

    n2 = klass(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries

    n3 = Node.from_page_data(mmap.PAGESIZE, 7, data)
    assert isinstance(n3, klass)
    assert n1._entries == n3._entries


def test_leaf_node_serialization():
    n1 = LeafNode(mmap.PAGESIZE, 7)
    n1.insert_entry(Record(43, 8083))
    n1.insert_entry(Record(42, 8082))
    assert n1._entries == [Record(42, 8082), Record(43, 8083)]
    data = n1.dump()

    n2 = LeafNode(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries


def test_root_node_serialization():
    n1 = RootNode(mmap.PAGESIZE, 7)
    n1.insert_entry(Reference(43, 2, 3))
    n1.insert_entry(Reference(42, 1, 2))
    assert n1._entries == [Reference(42, 1, 2), Reference(43, 2, 3)]
    data = n1.dump()

    n2 = RootNode(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries


def test_pairwise():
    l = [0, 1, 2, 3, 4]
    i = pairwise(l)
    assert next(i) == (0, 1)
    assert next(i) == (1, 2)
    assert next(i) == (2, 3)
    assert next(i) == (3, 4)
    with pytest.raises(StopIteration):
        next(i)
