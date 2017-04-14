import copy
from functools import partial
import mmap
import os.path
from typing import Optional, Union

from . import utils
from .const import ENDIAN, PAGE_REFERENCE_BYTES, OTHERS_BYTES, TreeConf
from .entry import Record, Reference
from .node import Node, LonelyRootNode, RootNode, InternalNode, LeafNode


class BPlusTree:

    def __init__(self, filename: Optional[str]=None,
                 page_size: int= 4096, order: int=4, key_size: int=16,
                 value_size: int=16):
        self._max_page = 1000000  # TODO: to remove
        if not filename:
            length = 5000 * page_size
            self._mm = mmap.mmap(-1, length, access=mmap.ACCESS_WRITE)
            self._create_init_values(page_size, order, key_size, value_size)
            self._write_page(0, self._dump_metadata())
        elif not os.path.exists(filename):
            self._mm = open(filename, mode='x+b', buffering=0)
            self._create_init_values(page_size, order, key_size, value_size)
            self._write_page(0, self._dump_metadata())
        else:
            self._mm = open(filename, mode='r+b', buffering=0)
            # Temporary tree conf used for loading the real one stored in
            # file metadata
            self._tree_conf = TreeConf(page_size, order, key_size, value_size)
            self._load_metadata(self._read_page(0))
            self._create_partials()

    def _create_partials(self):
        self.LonelyRootNode = partial(LonelyRootNode, self._tree_conf)
        self.RootNode = partial(RootNode, self._tree_conf)
        self.InternalNode = partial(InternalNode, self._tree_conf)
        self.LeafNode = partial(LeafNode, self._tree_conf)
        self.Record = partial(Record, self._tree_conf)
        self.Reference = partial(Reference, self._tree_conf)

    def close(self):
        self._mm.flush()
        self._mm.close()

    def _create_init_values(self, page_size, order, key_size, value_size):
        self._tree_conf = TreeConf(page_size, order, key_size, value_size)
        self._root_node_page = 1
        self._next_available_page = 2
        self._create_partials()
        self._write_node(self.LonelyRootNode(page=1))

    def _write_page(self, page: int, data: Union[bytes, bytearray]):
        assert 0 <= page <= self._max_page
        assert len(data) == self._tree_conf.page_size
        self._mm.seek(page * self._tree_conf.page_size)
        self._mm.write(data)

    def _write_node(self, node: 'Node'):
        data = node.dump()
        self._write_page(node.page, data)

    def _read_page(self, page: int) -> bytes:
        assert 0 <= page <= self._max_page
        start = page * self._tree_conf.page_size
        stop = start + self._tree_conf.page_size
        self._mm.seek(start)
        data = bytes()
        while self._mm.tell() < stop:
            read_data = self._mm.read(stop - self._mm.tell())
            if read_data == b'':
                raise ValueError('Read until the end of file')
            data += read_data
        assert len(data) == self._tree_conf.page_size
        return data

    def _get_node_at_page(self, page: int) -> 'Node':
        data = self._read_page(page)
        return Node.from_page_data(self._tree_conf, data=data, page=page)

    def _load_metadata(self, data: bytes):
        end_root_node_page = PAGE_REFERENCE_BYTES
        self._root_node_page = int.from_bytes(
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
        loaded_tree_conf = TreeConf(page_size, order, key_size, value_size)
        if loaded_tree_conf != self._tree_conf:
            raise ValueError('Configuration of tree does not match file')

    def _dump_metadata(self) -> bytes:
        length = PAGE_REFERENCE_BYTES + 4 * OTHERS_BYTES
        return (
            self._root_node_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN) +
            self._tree_conf.page_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.order.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.key_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            self._tree_conf.value_size.to_bytes(OTHERS_BYTES, ENDIAN) +
            bytes(self._tree_conf.page_size - length)
        )

    def _allocate_new_page(self) -> int:
        rv = copy.copy(self._next_available_page)
        self._next_available_page += 1
        self._write_page(0, self._dump_metadata())
        return rv

    @property
    def _root_node(self) -> Union['LonelyRootNode', 'RootNode']:
        root_node = self._get_node_at_page(self._root_node_page)
        assert isinstance(root_node, (LonelyRootNode, RootNode))
        return root_node

    def get(self, key) -> bytes:
        node = self._search_in_tree(key, self._root_node)
        record = node.get_entry(key)
        assert isinstance(record.value, bytes)
        return record.value

    def insert(self, key, value: bytes):

        if not isinstance(value, bytes):
            ValueError('Values must be bytes objects')

        node = self._search_in_tree(key, self._root_node)
        if node.can_add_entry:
            node.insert_entry(self.Record(key, value))
            self._write_node(node)
        else:
            node.insert_entry(self.Record(key, value))
            self._split_leaf(node)

    def _search_in_tree(self, key, node) -> 'Node':
        if isinstance(node, (LonelyRootNode, LeafNode)):
            return node

        page = None

        if key < node.smallest_key:
            page = node.smallest_entry.before

        elif node.biggest_key <= key:
            page = node.biggest_entry.after

        else:
            for ref_a, ref_b in utils.pairwise(node.entries):
                if ref_a.key <= key < ref_b.key:
                    page = ref_a.after
                    break

        assert page is not None

        child_node = self._get_node_at_page(page)
        child_node.parent = node
        return self._search_in_tree(key, child_node)

    def _split_leaf(self, old_node: 'Node'):
        parent = old_node.parent
        new_node = self.LeafNode(page=self._allocate_new_page())
        new_entries = old_node.split_entries()
        new_node.entries = new_entries
        ref = self.Reference(new_node.smallest_key,
                             old_node.page, new_node.page)

        if isinstance(old_node, LonelyRootNode):
            # Convert the LonelyRoot into a Leaf
            old_node = old_node.convert_to_leaf()
            self._create_new_root(ref)
        elif parent.can_add_entry:
            parent.insert_entry(ref)
            self._write_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        self._write_node(old_node)
        self._write_node(new_node)

    def _split_parent(self, old_node: Node):
        parent = old_node.parent
        new_node = self.InternalNode(page=self._allocate_new_page())
        new_entries = old_node.split_entries()
        new_node.entries = new_entries

        ref = new_node.pop_smallest()
        ref.before = old_node.page
        ref.after = new_node.page

        if isinstance(old_node, RootNode):
            # Convert the Root into an Internal
            old_node = old_node.convert_to_internal()
            self._create_new_root(ref)
        elif parent.can_add_entry:
            parent.insert_entry(ref)
            self._write_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        self._write_node(old_node)
        self._write_node(new_node)

    def _create_new_root(self, reference: Reference):
        new_root = self.RootNode(page=self._allocate_new_page())
        new_root.insert_entry(reference)
        self._root_node_page = new_root.page
        self._write_page(0, self._dump_metadata())
        self._write_node(new_root)
