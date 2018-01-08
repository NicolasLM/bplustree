from datetime import datetime, timezone, timedelta
import itertools
from unittest import mock
import uuid

import pytest

from bplustree.memory import FileMemory
from bplustree.node import LonelyRootNode, LeafNode
from bplustree.tree import BPlusTree
from bplustree.serializer import (
    IntSerializer, StrSerializer, UUIDSerializer, DatetimeUTCSerializer
)
from .conftest import filename


@pytest.fixture
def b():
    b = BPlusTree(filename, key_size=16, value_size=16, order=4)
    yield b
    b.close()


def test_create_and_load_file():
    b = BPlusTree(filename)
    assert isinstance(b._mem, FileMemory)
    b.insert(5, b'foo')
    b.close()

    b = BPlusTree(filename)
    assert isinstance(b._mem, FileMemory)
    assert b.get(5) == b'foo'
    b.close()


@mock.patch('bplustree.tree.BPlusTree.close')
def test_closing_context_manager(mock_close):
    with BPlusTree(filename, page_size=512, value_size=128) as b:
        pass
    mock_close.assert_called_once_with()


def test_initial_values():
    b = BPlusTree(filename, page_size=512, value_size=128)
    assert b._tree_conf.page_size == 512
    assert b._tree_conf.order == 100
    assert b._tree_conf.key_size == 8
    assert b._tree_conf.value_size == 128
    b.close()


def test_partial_constructors(b):
    node = b.RootNode()
    record = b.Record()
    assert node._tree_conf == b._tree_conf
    assert record._tree_conf == b._tree_conf


def test_insert_setitem_tree(b):
    b.insert(1, b'foo')

    with pytest.raises(ValueError):
        b.insert(1, b'bar')
    assert b.get(1) == b'foo'

    b.insert(1, b'baz', replace=True)
    assert b.get(1) == b'baz'

    b[1] = b'foo'
    assert b.get(1) == b'foo'


def test_get_tree(b):
    b.insert(1, b'foo')
    assert b.get(1) == b'foo'
    assert b.get(2) is None
    assert b.get(2, 'bar') == 'bar'


def test_getitem_tree(b):
    b.insert(1, b'foo')
    b.insert(2, b'bar')
    b.insert(5, b'baz')

    assert b[1] == b'foo'
    with pytest.raises(KeyError):
        _ = b[4]

    assert b[1:3] == {1: b'foo', 2: b'bar'}
    assert b[0:10] == {1: b'foo', 2: b'bar', 5: b'baz'}


def test_contains_tree(b):
    b.insert(1, b'foo')
    assert 1 in b
    assert 2 not in b


def test_len_tree(b):
    assert len(b) == 0
    b.insert(1, b'foo')
    assert len(b) == 1
    for i in range(2, 101):
        b.insert(i, str(i).encode())
    assert len(b) == 100


def test_length_hint_tree():
    b = BPlusTree(filename, key_size=16, value_size=16, order=100)
    assert b.__length_hint__() == 49
    b.insert(1, b'foo')
    assert b.__length_hint__() == 49
    for i in range(2, 10001):
        b.insert(i, str(i).encode())
    assert b.__length_hint__() == 7242
    b.close()


def test_bool_tree(b):
    assert not b
    b.insert(1, b'foo')
    assert b


def test_iter_keys_values_items_tree(b):
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

    # Test .keys()
    previous = 0
    for i in b.keys():
        assert i == previous + 1
        previous += 1

    # Test slice .keys()
    assert list(b.keys(slice(10, 13))) == [10, 11, 12]

    # Test .values()
    previous = 0
    for i in b.values():
        assert int(i.decode()) == previous + 1
        previous += 1

    # Test slice .values()
    assert list(b.values(slice(10, 13))) == [b'10', b'11', b'12']

    # Test .items()
    previous = 0
    for k, v in b.items():
        expected = previous + 1
        assert (k, int(v.decode())) == (expected, expected)
        previous += 1

    # Test slice .items()
    expected = [(10, b'10'), (11, b'11'), (12, b'12')]
    assert list(b.items(slice(10, 13))) == expected


def test_iter_slice(b):
    with pytest.raises(ValueError):
        next(b._iter_slice(slice(None, None, -1)))

    with pytest.raises(ValueError):
        next(b._iter_slice(slice(10, 0, None)))

    # Contains from 0 to 9 included
    for i in range(10):
        b.insert(i, str(i).encode())

    iter = b._iter_slice(slice(None, 2))
    assert next(iter).key == 0
    assert next(iter).key == 1
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(5, 7))
    assert next(iter).key == 5
    assert next(iter).key == 6
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(8, 9))
    assert next(iter).key == 8
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(9, 12))
    assert next(iter).key == 9
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(15, 17))
    with pytest.raises(StopIteration):
        next(iter)

    iter = b._iter_slice(slice(-2, 17))
    assert next(iter).key == 0

    b.close()

    # Contains from 10, 20, 30 .. 200
    b = BPlusTree(filename, order=5)
    for i in range(10, 201, 10):
        b.insert(i, str(i).encode())

    iter = b._iter_slice(slice(65, 85))
    assert next(iter).key == 70
    assert next(iter).key == 80
    with pytest.raises(StopIteration):
        next(iter)


def test_checkpoint(b):
    b.checkpoint()
    b.insert(1, b'foo')
    assert not b._mem._wal._not_committed_pages
    assert b._mem._wal._committed_pages

    b.checkpoint()
    assert not b._mem._wal._not_committed_pages
    assert not b._mem._wal._committed_pages


def test_left_record_node_in_tree():
    b = BPlusTree(filename, order=3)
    assert b._left_record_node == b._root_node
    assert isinstance(b._left_record_node, LonelyRootNode)
    b.insert(1, b'1')
    b.insert(2, b'2')
    b.insert(3, b'3')
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
values_sizes = [1, 16]
serializer_class = [IntSerializer, StrSerializer]
cache_sizes = [0, 50]
matrix = itertools.product(iterators, orders, page_sizes, key_sizes,
                           values_sizes, serializer_class, cache_sizes)


@pytest.mark.parametrize(
    'iterator,order,page_size,k_size,v_size,serialize_class,cache_size', matrix
)
def test_insert_split_in_tree(iterator, order, page_size, k_size, v_size,
                              serialize_class, cache_size):

    inserted = list()
    for i in iterator:
        v = str(i).encode()
        k = i
        if serialize_class is StrSerializer:
            k = str(i)
        inserted.append((k, v))

    b = BPlusTree(filename, order=order, page_size=page_size,
                  key_size=k_size, value_size=v_size, cache_size=cache_size,
                  serializer=serialize_class())

    if sorted(inserted) == inserted:
        b.batch_insert(inserted)
    else:
        for k, v in inserted:
            b.insert(k, v)

    # Reload tree from file before checking values
    b.close()
    b = BPlusTree(filename, order=order, page_size=page_size,
                  key_size=k_size, value_size=v_size, cache_size=cache_size,
                  serializer=serialize_class())

    for k, v in inserted:
        assert b.get(k) == v

    b.close()


def test_insert_split_in_tree_uuid():
    # Not in the test matrix because the iterators don't really make sense
    test_insert_split_in_tree(
        [uuid.uuid4() for _ in range(1000)],
        20,
        4096,
        16,
        40,
        UUIDSerializer,
        50
    )


def test_insert_split_in_tree_datetime_utc():
    dt = datetime(2018, 1, 6, 21, 42, 2, 424739, tzinfo=timezone.utc)
    test_insert_split_in_tree(
        [dt + timedelta(minutes=i) for i in range(1000)],
        20,
        2048,
        8,
        40,
        DatetimeUTCSerializer,
        50
    )


def test_overflow(b):
    data = b'f' * 323343
    with b._mem.write_transaction:
        first_overflow_page = b._create_overflow(data)
        assert b._read_from_overflow(first_overflow_page) == data

    with b._mem.read_transaction:
        assert b._read_from_overflow(first_overflow_page) == data


def test_batch_insert(b):
    def generate(from_, to):
        for i in range(from_, to):
            yield i, str(i).encode()

    b.batch_insert(generate(0, 1000))
    b.batch_insert(generate(1000, 2000))

    i = 0
    for k, v in b.items():
        assert k == i
        assert v == str(i).encode()
        i += 1
    assert i == 2000


def test_batch_insert_no_in_order(b):
    with pytest.raises(ValueError):
        b.batch_insert([(2, b'2'), (1, b'1')])
    assert b.get(1) is None
    assert b.get(2) is None

    b.insert(2, b'2')
    with pytest.raises(ValueError):
        b.batch_insert([(1, b'1')])

    with pytest.raises(ValueError):
        b.batch_insert([(2, b'2')])

    assert b.get(1) is None
    assert b.get(2) == b'2'
