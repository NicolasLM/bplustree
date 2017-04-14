import pytest

from bplustree.const import TreeConf
from bplustree.entry import Record, Reference
from bplustree.node import (Node, LonelyRootNode, RootNode, InternalNode,
                            LeafNode)

tree_conf = TreeConf(4096, 7, 16, 16)


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
    node = klass(TreeConf(4096, order, 16, 16))
    assert node._min_children == min_children
    assert node._max_children == max_children


@pytest.mark.parametrize('klass', [
    LeafNode, InternalNode, RootNode, LonelyRootNode,
])
def test_empty_node_serialization(klass):
    n1 = klass(tree_conf)
    data = n1.dump()

    n2 = klass(tree_conf, data=data)
    assert n1.entries == n2.entries

    n3 = Node.from_page_data(tree_conf, data)
    assert isinstance(n3, klass)
    assert n1.entries == n3.entries


def test_leaf_node_serialization():
    n1 = LeafNode(tree_conf)
    n1.insert_entry(Record(tree_conf, 43, b'43'))
    n1.insert_entry(Record(tree_conf, 42, b'42'))
    assert n1.entries == [Record(tree_conf, 42, b'42'),
                          Record(tree_conf, 43, b'43')]
    data = n1.dump()

    n2 = LeafNode(tree_conf, data=data)
    assert n1.entries == n2.entries


def test_root_node_serialization():
    n1 = RootNode(tree_conf)
    n1.insert_entry(Reference(tree_conf, 43, 2, 3))
    n1.insert_entry(Reference(tree_conf, 42, 1, 2))
    assert n1.entries == [Reference(tree_conf, 42, 1, 2),
                          Reference(tree_conf, 43, 2, 3)]
    data = n1.dump()

    n2 = RootNode(tree_conf, data=data)
    assert n1.entries == n2.entries
