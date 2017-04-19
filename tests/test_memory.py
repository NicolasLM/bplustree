import os

import pytest

from bplustree.node import LeafNode
from bplustree.memory import Memory, FileMemory
from bplustree.const import TreeConf

tree_conf = TreeConf(4096, 4, 16, 16)
node = LeafNode(tree_conf, page=3)


def test_memory_node():
    mem = Memory()

    with pytest.raises(ValueError):
        mem.get_node(3)

    mem.set_node(node)
    assert mem.get_node(3) == node

    mem.close()


def test_memory_metadata():
    mem = Memory()
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)


def test_memory_next_available_page():
    mem = Memory()
    for i in range(1, 100):
        assert mem.next_available_page == i


@pytest.fixture
def fd():
    filename = '/tmp/bplustree-testfile.index'
    fd = open(filename, mode='x+b', buffering=0)
    yield fd
    fd.close()
    if os.path.isfile(filename):
        os.unlink(filename)


def test_file_memory_node(fd):
    mem = FileMemory(fd, tree_conf)

    with pytest.raises(ValueError):
        mem.get_node(3)

    mem.set_node(node)
    rv = mem.get_node(3)
    print(node, rv)
    assert node == rv

    mem.close()


def test_file_memory_metadata(fd):
    mem = FileMemory(fd, tree_conf)
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)


def test_file_memory_next_available_page(fd):
    mem = FileMemory(fd, tree_conf)
    for i in range(1, 100):
        assert mem.next_available_page == i
