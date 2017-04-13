import itertools
import mmap

import pytest

from bplustree.const import ENDIAN
from bplustree.node import Node, RootNode
from bplustree.tree import BPlusTree


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
    b.insert(1, b'foo')
    assert b.get(1) == b'foo'
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
        b.insert(i, str(i).encode())
        inserted.add(i)

        if check_after_each_insert:
            for x in inserted:
                assert b.get(x) == str(x).encode()

    if not check_after_each_insert:
        for x in inserted:
            assert b.get(x) == str(x).encode()

    b.close()


def test_get_node_from_page_data():
    data = (2).to_bytes(1, ENDIAN) + bytes(mmap.PAGESIZE - 1)
    assert isinstance(
        Node.from_page_data(mmap.PAGESIZE, 4, data=data),
        RootNode
    )
