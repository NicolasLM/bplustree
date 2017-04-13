import mmap

import pytest

from bplustree.entry import Record, Reference
from bplustree.node import (Node, LonelyRootNode, RootNode, InternalNode,
                            LeafNode)


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


@pytest.mark.parametrize('klass', [
    LeafNode, InternalNode, RootNode, LonelyRootNode,
])
def test_empty_node_serialization(klass):
    n1 = klass(mmap.PAGESIZE, 7)
    data = n1.dump()

    n2 = klass(mmap.PAGESIZE, 7, data=data)
    assert n1.entries == n2.entries

    n3 = Node.from_page_data(mmap.PAGESIZE, 7, data)
    assert isinstance(n3, klass)
    assert n1.entries == n3.entries


def test_leaf_node_serialization():
    n1 = LeafNode(mmap.PAGESIZE, 7)
    n1.insert_entry(Record(43, 8083))
    n1.insert_entry(Record(42, 8082))
    assert n1.entries == [Record(42, 8082), Record(43, 8083)]
    data = n1.dump()

    n2 = LeafNode(mmap.PAGESIZE, 7, data=data)
    assert n1.entries == n2.entries


def test_root_node_serialization():
    n1 = RootNode(mmap.PAGESIZE, 7)
    n1.insert_entry(Reference(43, 2, 3))
    n1.insert_entry(Reference(42, 1, 2))
    assert n1.entries == [Reference(42, 1, 2), Reference(43, 2, 3)]
    data = n1.dump()

    n2 = RootNode(mmap.PAGESIZE, 7, data=data)
    assert n1.entries == n2.entries
