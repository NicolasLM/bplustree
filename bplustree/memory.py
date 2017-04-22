import enum
import io
import os
from typing import Union, Tuple

import cachetools

from .node import Node
from .const import ENDIAN, PAGE_REFERENCE_BYTES, OTHERS_BYTES, TreeConf


class Fsync(enum.Enum):
    ALWAYS = 1
    NEVER = 2


def open_file_in_dir(path: str) -> Tuple[io.FileIO, int]:
    """Open a file and its directory.

    The file is opened in binary mode and created if it does not exist.
    Both file descriptors must be closed after use to prevent them from
    leaking.
    """
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        raise ValueError('No directory {}'.format(directory))

    if not os.path.exists(path):
        file_fd = open(path, mode='x+b', buffering=0)
    else:
        file_fd = open(path, mode='r+b', buffering=0)

    dir_fd = os.open(directory, os.O_RDONLY)

    return file_fd, dir_fd


class Memory:

    def __init__(self):
        self._nodes = dict()
        self._metadata = dict()
        self.last_page = 0

    def get_node(self, page: int):
        try:
            return self._nodes[page]
        except KeyError:
            raise ValueError('No node at page {}'.format(page))

    def set_node(self, node: Node):
        self._nodes[node.page] = node

    def get_metadata(self) -> tuple:
        try:
            rv = self._metadata['root_node_page'], self._metadata['tree_conf']
            return rv
        except KeyError:
            raise ValueError('Metadata not set yet')

    def set_metadata(self, root_node_page: int, tree_conf: TreeConf):
        self._metadata['root_node_page'] = root_node_page
        self._metadata['tree_conf'] = tree_conf

    @property
    def next_available_page(self) -> int:
        self.last_page += 1
        return self.last_page

    def close(self):
        pass


class FileMemory(Memory):

    def __init__(self, filename: str, tree_conf: TreeConf,
                 cache_size: int=1000, fsync: Fsync=Fsync.ALWAYS):
        super().__init__()
        self._fd, self._dir_fd = open_file_in_dir(filename)
        self._tree_conf = tree_conf
        self._cache = cachetools.LRUCache(cache_size)
        self.fsync = fsync

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)

    def get_node(self, page: int):
        try:
            return self._cache[page]
        except KeyError:
            data = self._read_page(page)
            return Node.from_page_data(self._tree_conf, data=data, page=page)

    def set_node(self, node: Node):
        data = node.dump()
        self._write_page(node.page, data)
        self._cache[node.page] = node

    def get_metadata(self) -> tuple:
        try:
            data = self._read_page(0)
        except ValueError:
            raise ValueError('Metadata not set yet')
        end_root_node_page = PAGE_REFERENCE_BYTES
        root_node_page = int.from_bytes(
            data[0:end_root_node_page], ENDIAN
        )
        end_page_size = end_root_node_page + OTHERS_BYTES
        page_size = int.from_bytes(
            data[end_root_node_page:end_page_size], ENDIAN
        )
        end_order = end_page_size + OTHERS_BYTES
        order = int.from_bytes(
            data[end_page_size:end_order], ENDIAN
        )
        end_key_size = end_order + OTHERS_BYTES
        key_size = int.from_bytes(
            data[end_order:end_key_size], ENDIAN
        )
        end_value_size = end_key_size + OTHERS_BYTES
        value_size = int.from_bytes(
            data[end_key_size:end_value_size], ENDIAN
        )
        self._tree_conf = TreeConf(page_size, order, key_size, value_size)
        return root_node_page, self._tree_conf

    def set_metadata(self, root_node_page: int, tree_conf: TreeConf):
        self._tree_conf = tree_conf
        length = PAGE_REFERENCE_BYTES + 4 * OTHERS_BYTES
        data = (
            root_node_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN) +
            self._tree_conf.page_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.order.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.key_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.value_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            bytes(self._tree_conf.page_size - length)
        )
        self._write_page(0, data)

    def close(self):
        self._fd.close()
        os.close(self._dir_fd)

    def _read_page(self, page: int) -> bytes:
        start = page * self._tree_conf.page_size
        stop = start + self._tree_conf.page_size
        self._fd.seek(start)
        data = bytes()
        while self._fd.tell() < stop:
            read_data = self._fd.read(stop - self._fd.tell())
            if read_data == b'':
                raise ValueError('Read until the end of file')
            data += read_data
        assert len(data) == self._tree_conf.page_size
        return data

    def _write_page(self, page: int, data: Union[bytes, bytearray]):
        assert len(data) == self._tree_conf.page_size
        self._fd.seek(page * self._tree_conf.page_size)
        self._fd.write(data)
        if self.fsync == Fsync.ALWAYS:
            os.fsync(self._fd.fileno())
            os.fsync(self._dir_fd)
