import itertools
import os

import pytest

from bplustree.memory import Memory, FileMemory, Fsync
from bplustree.node import LonelyRootNode, LeafNode
from bplustree.tree import BPlusTree

filename = '/tmp/bplustree-testfile.index'


@pytest.fixture
def clean_file():
    if os.path.isfile(filename):
        os.unlink(filename)
    yield
    if os.path.isfile(filename):
        os.unlink(filename)


def test_create_in_memory():
    b = BPlusTree()
    assert isinstance(b._mem, Memory)
    b.close()


def test_create_and_load_file(clean_file):
    b = BPlusTree(filename=filename)
    assert isinstance(b._mem, FileMemory)
    b.insert(5, b'foo')
    b.close()

    b = BPlusTree(filename=filename)
    assert isinstance(b._mem, FileMemory)
    assert b.get(5) == b'foo'
    b.close()


def test_initial_values():
    b = BPlusTree(page_size=512, value_size=128)
    assert b._tree_conf.page_size == 512
    assert b._tree_conf.order == 4
    assert b._tree_conf.key_size == 16
    assert b._tree_conf.value_size == 128
    b.close()


def test_partial_constructors():
    b = BPlusTree()
    node = b.RootNode()
    record = b.Record()
    assert node._tree_conf == b._tree_conf
    assert record._tree_conf == b._tree_conf
    b.close()


def test_insert_get_record_in_tree():
    b = BPlusTree()
    b.insert(1, b'foo')
    assert b.get(1) == b'foo'
    b.close()


def test_iter_tree():
    b = BPlusTree(order=3)

    # Empty tree
    iter = b.__iter__()
    with pytest.raises(StopIteration):
        next(iter)

    # Insert in reverse...
    for i in range(1000, 0, -1):
        b.insert(i, str(i).encode())
    # ...iter in order
    previous = 0
    for i in b:
        assert i == previous + 1
        previous += 1

    b.close()


def test_iter_slice():
    b = BPlusTree(order=3)

    with pytest.raises(ValueError):
        next(b._iter_slice(slice(None, None, -1)))

    with pytest.raises(ValueError):
        next(b._iter_slice(slice(10, 0, None)))

    # Contains from 0 to 9 included
    for i in range(10):
        b.insert(i, str(i).encode())

    iter = b._iter_slice(slice(None, 2))
    assert next(iter) == 0
    assert next(iter) == 1
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(5, 7))
    assert next(iter) == 5
    assert next(iter) == 6
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(8, 9))
    assert next(iter) == 8
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(9, 12))
    assert next(iter) == 9
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(15, 17))
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(-2, 17))
    assert next(iter) == 0

    b.close()

    # Contains from 10, 20, 30 .. 200
    b = BPlusTree(order=5)
    for i in range(10, 201, 10):
        b.insert(i, str(i).encode())

    iter = b._iter_slice(slice(65, 85))
    assert next(iter) == 70
    assert next(iter) == 80
    with pytest.raises(StopIteration):
        next(iter)

    b.close()


def test_left_record_node_in_tree():
    b = BPlusTree(order=3)
    assert b._left_record_node == b._root_node
    assert isinstance(b._left_record_node, LonelyRootNode)
    b.insert(1, b'1')
    b.insert(2, b'2')
    b.insert(2, b'2')
    assert isinstance(b._left_record_node, LeafNode)
    b.close()

iterators = [
    range(0, 1000, 1),
    range(1000, 0, -1),
    list(range(0, 1000, 2)) + list(range(1, 1000, 2))
]
orders = [3, 4, 50]
page_sizes = [4096, 8192]
key_sizes = [4, 16]
values_sizes = [4, 16]
file_names = [None, filename]
matrix = itertools.product(iterators, orders, page_sizes,
                           key_sizes, values_sizes, file_names)


@pytest.mark.parametrize('iterator,order,page_size,k_size,v_size,filename',
                         matrix)
def test_insert_split_in_tree(iterator, order, page_size, k_size, v_size,
                              filename, clean_file):
    inserted = set()

    b = BPlusTree(filename=filename, order=order, page_size=page_size,
                  key_size=k_size, value_size=v_size, fsync=Fsync.NEVER)

    for i in iterator:
        b.insert(i, str(i).encode())
        inserted.add(i)

    if filename:
        # Reload tree from file before checking values
        b.close()
        b = BPlusTree(filename=filename, order=order, page_size=page_size,
                      key_size=k_size, value_size=v_size)

    for x in inserted:
        assert b.get(x) == str(x).encode()

    b.close()
