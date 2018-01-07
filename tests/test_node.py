import pytest

from bplustree.const import TreeConf, ENDIAN
from bplustree.entry import Record, Reference
from bplustree.node import (Node, LonelyRootNode, RootNode, InternalNode,
                            LeafNode)
from bplustree.serializer import IntSerializer

tree_conf = TreeConf(4096, 7, 16, 16, IntSerializer())


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
    node = klass(TreeConf(4096, order, 16, 16, IntSerializer()))
    assert node.min_children == min_children
    assert node.max_children == max_children


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
    n1 = LeafNode(tree_conf, next_page=66)
    n1.insert_entry(Record(tree_conf, 43, b'43'))
    n1.insert_entry(Record(tree_conf, 42, b'42'))
    assert n1.entries == [Record(tree_conf, 42, b'42'),
                          Record(tree_conf, 43, b'43')]
    data = n1.dump()

    n2 = LeafNode(tree_conf, data=data)
    assert n1.entries == n2.entries
    assert n1.next_page == n2.next_page == 66


def test_leaf_node_serialization_no_next_page():
    n1 = LeafNode(tree_conf)
    data = n1.dump()

    n2 = LeafNode(tree_conf, data=data)
    assert n1.next_page is n2.next_page is None


def test_root_node_serialization():
    n1 = RootNode(tree_conf)
    n1.insert_entry(Reference(tree_conf, 43, 2, 3))
    n1.insert_entry(Reference(tree_conf, 42, 1, 2))
    assert n1.entries == [Reference(tree_conf, 42, 1, 2),
                          Reference(tree_conf, 43, 2, 3)]
    data = n1.dump()

    n2 = RootNode(tree_conf, data=data)
    assert n1.entries == n2.entries
    assert n1.next_page is n2.next_page is None


def test_node_slots():
    n1 = RootNode(tree_conf)
    with pytest.raises(AttributeError):
        n1.foo = True


def test_get_node_from_page_data():
    data = (2).to_bytes(1, ENDIAN) + bytes(4096 - 1)
    tree_conf = TreeConf(4096, 7, 16, 16, IntSerializer())
    assert isinstance(
        Node.from_page_data(tree_conf, data, 4),
        RootNode
    )


def test_insert_find_get_remove_entries():
    node = RootNode(tree_conf)

    # Test empty _find_entry_index, get and remove
    with pytest.raises(ValueError):
        node._find_entry_index(42)
    with pytest.raises(ValueError):
        node.get_entry(42)
    with pytest.raises(ValueError):
        node.remove_entry(42)

    # Test insert_entry
    r42, r43 = Reference(tree_conf, 42, 1, 2), Reference(tree_conf, 43, 2, 3)
    node.insert_entry_at_the_end(r43)
    node.insert_entry(r42)
    assert sorted(node.entries) == node.entries

    # Test _find_entry_index
    assert node._find_entry_index(42) == 0
    assert node._find_entry_index(43) == 1

    # Test _get_entry
    assert node.get_entry(42) == r42
    assert node.get_entry(43) == r43

    node.remove_entry(43)
    assert node.entries == [r42]
    node.remove_entry(42)
    assert node.entries == []


def test_smallest_biggest():
    node = RootNode(tree_conf)

    with pytest.raises(IndexError):
        node.pop_smallest()

    r42, r43 = Reference(tree_conf, 42, 1, 2), Reference(tree_conf, 43, 2, 3)
    node.insert_entry(r43)
    node.insert_entry(r42)

    # Smallest
    assert node.smallest_entry == r42
    assert node.smallest_key == 42

    # Biggest
    assert node.biggest_entry == r43
    assert node.biggest_key == 43

    assert node.pop_smallest() == r42
    assert node.entries == [r43]
