import mmap

import pytest

import bplustree


def test_create_in_memory():
    b = bplustree.BPlusTree()
    assert b._fd is None
    b.close()


def test_write_read_page():
    b = bplustree.BPlusTree()
    expected = b'abcdefgh' * int(mmap.PAGESIZE / 8)
    b._write_page(1, expected)
    assert b._read_page(1) == expected
    b.close()


def test_partial_node():
    b = bplustree.BPlusTree()
    node = b.RootNode()
    assert node._page_size == b._page_size
    assert node._order == b._order
    b.close()


def test_insert_get_record_in_tree():
    b = bplustree.BPlusTree()
    b.insert(1, 1024)
    assert b.get(1) == 1024
    b.close()


@pytest.mark.parametrize('iterator,check_after_each_insert', [
    (range(0, 10, 1), True),
    (range(0, 1000, 1), False),
    (range(10, 0, -1), True),
    (range(1000, 0, -1), False),
])
def test_insert_split_in_tree(iterator, check_after_each_insert):
    inserted = set()
    b = bplustree.BPlusTree()

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


def test_node_limit_children():
    lonely_root = bplustree.LonelyRootNode(mmap.PAGESIZE, 7)
    assert lonely_root._min_children == 0
    assert lonely_root._max_children == 6
    lonely_root = bplustree.LonelyRootNode(mmap.PAGESIZE, 100)
    assert lonely_root._min_children == 0
    assert lonely_root._max_children == 99

    root = bplustree.RootNode(mmap.PAGESIZE, 7)
    assert root._min_children == 2
    assert root._max_children == 7
    root = bplustree.RootNode(mmap.PAGESIZE, 100)
    assert root._min_children == 2
    assert root._max_children == 100

    internal = bplustree.InternalNode(mmap.PAGESIZE, 7)
    assert internal._min_children == 4
    assert internal._max_children == 7
    internal = bplustree.InternalNode(mmap.PAGESIZE, 100)
    assert internal._min_children == 50
    assert internal._max_children == 100

    leaf = bplustree.LeafNode(mmap.PAGESIZE, 7)
    assert leaf._min_children == 3
    assert leaf._max_children == 6
    leaf = bplustree.LeafNode(mmap.PAGESIZE, 100)
    assert leaf._min_children == 49
    assert leaf._max_children == 99


def test_get_node_from_page_data():
    data = (2).to_bytes(1, bplustree.ENDIAN) + bytes(mmap.PAGESIZE - 1)
    assert isinstance(
        bplustree.Node.from_page_data(mmap.PAGESIZE, 4, data=data),
        bplustree.RootNode
    )


def test_record_serialization():
    r1 = bplustree.Record(42, 8080)
    data = r1.dump()

    r2 = bplustree.Record(data=data)
    assert r1 == r2
    assert r1.value == r2.value


def test_reference_serialization():
    r1 = bplustree.Reference(42, 1, 2)
    data = r1.dump()

    r2 = bplustree.Reference(data=data)
    assert r1 == r2
    assert r1.before == r2.before
    assert r1.after == r2.after


@pytest.mark.parametrize('klass', [
    bplustree.LeafNode,
    bplustree.InternalNode,
    bplustree.RootNode,
    bplustree.LonelyRootNode,
])
def test_empty_node_serialization(klass):
    n1 = klass(mmap.PAGESIZE, 7)
    data = n1.dump()

    n2 = klass(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries

    n3 = bplustree.Node.from_page_data(mmap.PAGESIZE, 7, data)
    assert isinstance(n3, klass)
    assert n1._entries == n3._entries


def test_leaf_node_serialization():
    n1 = bplustree.LeafNode(mmap.PAGESIZE, 7)
    n1.insert_entry(bplustree.Record(43, 8083))
    n1.insert_entry(bplustree.Record(42, 8082))
    assert n1._entries == [bplustree.Record(42, 8082),
                           bplustree.Record(43, 8083)]
    data = n1.dump()

    n2 = bplustree.LeafNode(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries


def test_root_node_serialization():
    n1 = bplustree.RootNode(mmap.PAGESIZE, 7)
    n1.insert_entry(bplustree.Reference(43, 2, 3))
    n1.insert_entry(bplustree.Reference(42, 1, 2))
    assert n1._entries == [bplustree.Reference(42, 1, 2),
                           bplustree.Reference(43, 2, 3)]
    data = n1.dump()

    n2 = bplustree.RootNode(mmap.PAGESIZE, 7, data=data)
    assert n1._entries == n2._entries


def test_pairwise():
    l = [0, 1, 2, 3, 4]
    i = bplustree.pairwise(l)
    assert next(i) == (0, 1)
    assert next(i) == (1, 2)
    assert next(i) == (2, 3)
    assert next(i) == (3, 4)
    with pytest.raises(StopIteration):
        next(i)
