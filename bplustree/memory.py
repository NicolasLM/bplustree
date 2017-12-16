import enum
import io
from logging import getLogger
import os
from typing import Union, Tuple, Optional

from .node import Node
from .const import (
    ENDIAN, PAGE_REFERENCE_BYTES, OTHERS_BYTES, TreeConf,
    FRAME_TYPE_BYTES, TRANSACTION_ID_BYTES
)

logger = getLogger(__name__)


class Fsync(enum.Enum):
    ALWAYS = 1
    NEVER = 2


class ReachedEndOfFile(Exception):
    """Read a file until its end."""


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


def write_to_file(file_fd: io.FileIO, dir_fileno: int,
                  data: bytes, fsync: bool):
    length_to_write = len(data)
    written = 0
    while written < length_to_write:
        written = file_fd.write(data[written:])
    if fsync:
        fsync_file_and_dir(file_fd.fileno(), dir_fileno)


def fsync_file_and_dir(file_fileno: int, dir_fileno: int):
    return
    os.fsync(file_fileno)
    os.fsync(dir_fileno)


def read_from_file(file_fd: io.FileIO, start: int, stop: int) -> bytes:
    length = stop - start
    assert length >= 0
    file_fd.seek(start)
    data = bytes()
    while file_fd.tell() < stop:
        read_data = file_fd.read(stop - file_fd.tell())
        if read_data == b'':
            raise ReachedEndOfFile('Read until the end of file')
        data += read_data
    assert len(data) == length
    return data


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
        self._wal = WAL(filename, tree_conf.page_size)
        self._tree_conf = tree_conf
        self.fsync = fsync

        # Get the next available page
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)

    def get_node(self, page: int):
        data = self._wal.get_page(page)
        if not data:
            data = self._read_page(page)
        return Node.from_page_data(self._tree_conf, data=data, page=page)

    def set_node(self, node: Node):
        self._wal.set_page(node.page, node.dump())
        self._wal.commit()

    def get_metadata(self) -> tuple:
        try:
            data = self._read_page(0)
        except ReachedEndOfFile:
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
        self._tree_conf = TreeConf(
            page_size, order, key_size, value_size, self._tree_conf.serializer
        )
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
        self._write_page_in_tree(0, data)

    def close(self):
        for page, page_data in self._wal.checkpoint():
            self._write_page_in_tree(page, page_data)
        fsync_file_and_dir(self._fd.fileno(), self._dir_fd)
        self._fd.close()
        os.close(self._dir_fd)

    def _read_page(self, page: int) -> bytes:
        start = page * self._tree_conf.page_size
        stop = start + self._tree_conf.page_size
        assert stop - start == self._tree_conf.page_size
        return read_from_file(self._fd, start, stop)

    def _write_page_in_tree(self, page: int, data: Union[bytes, bytearray]):
        """Write a page of data in the tree file itself.

        To be used during checkpoints and other non-standard uses.
        """
        assert len(data) == self._tree_conf.page_size
        self._fd.seek(page * self._tree_conf.page_size)
        write_to_file(self._fd, self._dir_fd, data, self.fsync is Fsync.ALWAYS)


class FrameType(enum.Enum):
    PAGE = 1
    COMMIT = 2
    ROLLBACK = 3


class WAL:

    FRAME_HEADER_LENGTH = (
        FRAME_TYPE_BYTES + TRANSACTION_ID_BYTES + PAGE_REFERENCE_BYTES
    )

    def __init__(self, filename: str, page_size: int):
        self.filename = filename + '-wal'
        self._fd, self._dir_fd = open_file_in_dir(self.filename)
        self._page_size = page_size
        self._committed_pages = dict()
        self._not_committed_pages = dict()

        self._fd.seek(0, io.SEEK_END)
        if self._fd.tell() == 0:
            self._create_header()
        else:
            logger.warning('Found an existing WAL file, '
                           'the B+Tree was not closed properly')
            self._load_wal()

    def checkpoint(self):
        if self._not_committed_pages:
            logger.warning('Closing WAL with uncommitted data, discarding it')

        fsync_file_and_dir(self._fd.fileno(), self._dir_fd)

        for page, page_start in self._committed_pages.items():
            page_data = read_from_file(
                self._fd,
                page_start,
                page_start + self._page_size
            )
            yield page, page_data

        self._fd.close()
        os.unlink(self.filename)
        os.fsync(self._dir_fd)
        os.close(self._dir_fd)

    def _create_header(self):
        data = self._page_size.to_bytes(OTHERS_BYTES, ENDIAN)
        self._fd.seek(0)
        write_to_file(self._fd, self._dir_fd, data, True)

    def _load_wal(self):
        self._fd.seek(0)
        header_data = read_from_file(self._fd, 0, OTHERS_BYTES)
        assert int.from_bytes(header_data, ENDIAN) == self._page_size

        while True:
            try:
                self._load_next_frame()
            except ReachedEndOfFile:
                break
        if self._not_committed_pages:
            logger.warning('WAL has uncommitted data, discarding it')
            self._not_committed_pages = dict()

    def _load_next_frame(self):
        start = self._fd.tell()
        stop = start + self.FRAME_HEADER_LENGTH
        data = read_from_file(self._fd, start, stop)

        frame_type = int.from_bytes(data[0:FRAME_TYPE_BYTES], ENDIAN)
        transaction_id = int.from_bytes(
            data[FRAME_TYPE_BYTES:FRAME_TYPE_BYTES+TRANSACTION_ID_BYTES],
            ENDIAN
        )
        page = int.from_bytes(
            data[FRAME_TYPE_BYTES+TRANSACTION_ID_BYTES:
                 FRAME_TYPE_BYTES+TRANSACTION_ID_BYTES+PAGE_REFERENCE_BYTES],
            ENDIAN
        )

        frame_type = FrameType(frame_type)
        if frame_type is FrameType.PAGE:
            self._fd.seek(stop + self._page_size)

        self._index_frame(frame_type, page, stop)

    def _index_frame(self, frame_type: FrameType, page: int, page_start: int):
        if frame_type is FrameType.PAGE:
            self._not_committed_pages[page] = page_start
        elif frame_type is FrameType.COMMIT:
            self._committed_pages.update(self._not_committed_pages)
            self._not_committed_pages = dict()
        elif frame_type is FrameType.ROLLBACK:
            self._not_committed_pages = dict()
        else:
            assert False

    def _add_frame(self, frame_type: FrameType, transaction_id: int,
                   page: Optional[int]=None, page_data: Optional[bytes]=None):
        if frame_type is FrameType.PAGE and (not page or not page_data):
            raise ValueError('PAGE frame without page data')
        if page_data and len(page_data) != self._page_size:
            raise ValueError('Page data is different from page size')
        if not page:
            page = 0
        if frame_type is not FrameType.PAGE:
            page_data = b''
        data = (
            frame_type.value.to_bytes(FRAME_TYPE_BYTES, ENDIAN) +
            transaction_id.to_bytes(TRANSACTION_ID_BYTES, ENDIAN) +
            page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN) +
            page_data
        )
        self._fd.seek(0, io.SEEK_END)
        write_to_file(self._fd, self._dir_fd, data,
                      fsync=frame_type != FrameType.PAGE)
        self._index_frame(frame_type, page, self._fd.tell() - self._page_size)

    def get_page(self, page: int) -> Optional[bytes]:
        page_start = None
        for store in (self._not_committed_pages, self._committed_pages):
            page_start = store.get(page)
            if page_start:
                break

        if not page_start:
            return None

        return read_from_file(self._fd, page_start,
                              page_start + self._page_size)

    def set_page(self, page: int, page_data: bytes):
        self._add_frame(FrameType.PAGE, 0, page, page_data)

    def commit(self):
        self._add_frame(FrameType.COMMIT, 0)

    def rollback(self):
        self._add_frame(FrameType.ROLLBACK, 0)

    def __repr__(self):
        return '<WAL: {}>'.format(self.filename)
