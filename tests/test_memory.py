import io
import os
from unittest import mock

import pytest

from bplustree.node import LeafNode
from bplustree.memory import Memory, FileMemory, Fsync, open_file_in_dir
from bplustree.const import TreeConf

tree_conf = TreeConf(4096, 4, 16, 16)
node = LeafNode(tree_conf, page=3)
filename = '/tmp/bplustree-testfile.index'


@pytest.fixture
def clean_file():
    if os.path.isfile(filename):
        os.unlink(filename)
    yield
    if os.path.isfile(filename):
        os.unlink(filename)


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


def test_file_memory_node(clean_file):
    mem = FileMemory(filename, tree_conf)

    with pytest.raises(ValueError):
        mem.get_node(3)

    mem.set_node(node)
    rv = mem.get_node(3)
    print(node, rv)
    assert node == rv

    mem.close()


def test_file_memory_metadata(clean_file):
    mem = FileMemory(filename, tree_conf)
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)


def test_file_memory_next_available_page(clean_file):
    mem = FileMemory(filename, tree_conf)
    for i in range(1, 100):
        assert mem.next_available_page == i


@mock.patch('bplustree.memory.os.fsync')
def test_file_memory_fsync(mock_fsync, clean_file):
    mem = FileMemory(filename, tree_conf, fsync=Fsync.NEVER)
    mem._write_page(0, bytes(tree_conf.page_size))
    mem.close()
    mock_fsync.assert_not_called()

    mem = FileMemory(filename, tree_conf, fsync=Fsync.ALWAYS)
    mem._write_page(0, bytes(tree_conf.page_size))
    mem.close()
    mock_fsync.assert_called_with(mock.ANY)


def test_open_file_in_dir(clean_file):
    with pytest.raises(ValueError):
        open_file_in_dir('/foo/bar/does/not/exist')

    # Create file and re-open
    for _ in range(2):
        file_fd, dir_fd = open_file_in_dir(filename)
        assert isinstance(file_fd, io.FileIO)
        assert isinstance(dir_fd, int)
        file_fd.close()
        os.close(dir_fd)
