from functools import partial
from typing import Optional, Union

from . import utils
from .const import TreeConf
from .entry import Record, Reference
from .memory import Memory, FileMemory, Fsync
from .node import Node, LonelyRootNode, RootNode, InternalNode, LeafNode


class BPlusTree:

    def __init__(self, filename: Optional[str]=None,
                 page_size: int= 4096, order: int=4, key_size: int=16,
                 value_size: int=16, cache_size: int=1000,
                 fsync: Fsync=Fsync.ALWAYS):
        self._tree_conf = TreeConf(page_size, order, key_size, value_size)
        self._create_partials()
        if not filename:
            self._mem = Memory()
            self._initialize_empty_tree()
        else:
            self._mem = FileMemory(
                filename, self._tree_conf, cache_size=cache_size,
                fsync=fsync
            )
            try:
                metadata = self._mem.get_metadata()
            except ValueError:
                self._initialize_empty_tree()
            else:
                self._root_node_page, self._tree_conf = metadata

    def _initialize_empty_tree(self):
        self._root_node_page = self._mem.next_available_page
        self._mem.set_node(self.LonelyRootNode(page=self._root_node_page))
        self._mem.set_metadata(self._root_node_page, self._tree_conf)

    def _create_partials(self):
        self.LonelyRootNode = partial(LonelyRootNode, self._tree_conf)
        self.RootNode = partial(RootNode, self._tree_conf)
        self.InternalNode = partial(InternalNode, self._tree_conf)
        self.LeafNode = partial(LeafNode, self._tree_conf)
        self.Record = partial(Record, self._tree_conf)
        self.Reference = partial(Reference, self._tree_conf)

    def close(self):
        self._mem.close()

    @property
    def _root_node(self) -> Union['LonelyRootNode', 'RootNode']:
        root_node = self._mem.get_node(self._root_node_page)
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
            self._mem.set_node(node)
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

        child_node = self._mem.get_node(page)
        child_node.parent = node
        return self._search_in_tree(key, child_node)

    def _split_leaf(self, old_node: 'Node'):
        parent = old_node.parent
        new_node = self.LeafNode(page=self._mem.next_available_page)
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
            self._mem.set_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        self._mem.set_node(old_node)
        self._mem.set_node(new_node)

    def _split_parent(self, old_node: Node):
        parent = old_node.parent
        new_node = self.InternalNode(page=self._mem.next_available_page)
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
            self._mem.set_node(parent)
        else:
            parent.insert_entry(ref)
            self._split_parent(parent)

        self._mem.set_node(old_node)
        self._mem.set_node(new_node)

    def _create_new_root(self, reference: Reference):
        new_root = self.RootNode(page=self._mem.next_available_page)
        new_root.insert_entry(reference)
        self._root_node_page = new_root.page
        self._mem.set_metadata(self._root_node_page, self._tree_conf)
        self._mem.set_node(new_root)
