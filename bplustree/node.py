import abc
import bisect
import math
from typing import Optional

from .const import (ENDIAN, NODE_TYPE_BYTES, USED_PAGE_LENGTH_BYTES,
                    PAGE_REFERENCE_BYTES, TreeConf)
from .entry import Entry, Record, Reference


class Node(metaclass=abc.ABCMeta):

    __slots__ = ['_tree_conf', 'entries', 'page', 'parent', 'next_page']

    # Attributes to redefine in inherited classes
    _node_type_int = 0
    max_children = 0
    min_children = 0
    _entry_class = None

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None, next_page: int=None):
        self._tree_conf = tree_conf
        self.entries = list()
        self.page = page
        self.parent = parent
        self.next_page = next_page
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self._tree_conf.page_size
        end_used_page_length = NODE_TYPE_BYTES + USED_PAGE_LENGTH_BYTES
        used_page_length = int.from_bytes(
            data[NODE_TYPE_BYTES:end_used_page_length], ENDIAN
        )
        end_header = end_used_page_length + PAGE_REFERENCE_BYTES
        self.next_page = int.from_bytes(
            data[end_used_page_length:end_header], ENDIAN
        )
        if self.next_page == 0:
            self.next_page = None

        entry_length = self._entry_class(self._tree_conf).length
        for start_offset in range(end_header, used_page_length, entry_length):
            entry_data = data[start_offset:start_offset+entry_length]
            entry = self._entry_class(self._tree_conf, data=entry_data)
            self.entries.append(entry)

    def dump(self) -> bytearray:
        data = bytearray()
        for record in self.entries:
            data.extend(record.dump())

        used_page_length = len(data) + 4
        assert 0 <= used_page_length < self._tree_conf.page_size
        next_page = 0 if self.next_page is None else self.next_page
        header = (
            self._node_type_int.to_bytes(1, ENDIAN) +
            used_page_length.to_bytes(3, ENDIAN) +
            next_page.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
        )

        data = bytearray(header) + data

        padding = self._tree_conf.page_size - len(data)
        assert padding >= 0
        data.extend(bytearray(padding))
        assert len(data) == self._tree_conf.page_size

        return data

    @property
    def can_add_entry(self) -> bool:
        return self.num_children < self.max_children

    @property
    def can_delete_entry(self) -> bool:
        return self.num_children > self.min_children

    @property
    def smallest_key(self):
        return self.smallest_entry.key

    @property
    def smallest_entry(self):
        return self.entries[0]

    @property
    def biggest_key(self):
        return self.biggest_entry.key

    @property
    def biggest_entry(self):
        return self.entries[-1]

    @property
    @abc.abstractmethod
    def num_children(self) -> int:
        """Number of entries or other nodes connected to the node."""

    def pop_smallest(self) -> Entry:
        """Remove and return the smallest entry."""
        return self.entries.pop(0)

    def insert_entry(self, entry: Entry):
        bisect.insort(self.entries, entry)

    def insert_entry_at_the_end(self, entry: Entry):
        """Insert an entry at the end of the entry list.

        This is an optimized version of `insert_entry` when it is known that
        the key to insert is bigger than any other entries.
        """
        self.entries.append(entry)

    def remove_entry(self, key):
        self.entries.pop(self._find_entry_index(key))

    def get_entry(self, key) -> Entry:
        return self.entries[self._find_entry_index(key)]

    def _find_entry_index(self, key) -> int:
        entry = self._entry_class(
            self._tree_conf,
            key=key  # Hack to compare and order
        )
        i = bisect.bisect_left(self.entries, entry)
        if i != len(self.entries) and self.entries[i] == entry:
            return i
        raise ValueError('No entry for key {}'.format(key))

    def split_entries(self) -> list:
        """Split the entries in half.

        Keep the lower part in the node and return the upper one.
        """
        len_entries = len(self.entries)
        rv = self.entries[len_entries//2:]
        self.entries = self.entries[:len_entries//2]
        assert len(self.entries) + len(rv) == len_entries
        return rv

    @classmethod
    def from_page_data(cls, tree_conf: TreeConf, data: bytes,
                       page: int=None) -> 'Node':
        node_type_byte = data[0:NODE_TYPE_BYTES]
        node_type_int = int.from_bytes(node_type_byte, ENDIAN)
        if node_type_int == 1:
            return LonelyRootNode(tree_conf, data, page)
        elif node_type_int == 2:
            return RootNode(tree_conf, data, page)
        elif node_type_int == 3:
            return InternalNode(tree_conf, data, page)
        elif node_type_int == 4:
            return LeafNode(tree_conf, data, page)
        else:
            assert False, 'No Node with type {} exists'.format(node_type_int)

    def __repr__(self):
        return '<{}: page={} entries={}>'.format(
            self.__class__.__name__, self.page, len(self.entries)
        )

    def __eq__(self, other):
        return (
            self.__class__ is other.__class__ and
            self.page == other.page and
            self.entries == other.entries
        )


class RecordNode(Node):

    __slots__ = ['_entry_class']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None, next_page: int=None):
        self._entry_class = Record
        super().__init__(tree_conf, data, page, parent, next_page)

    @property
    def num_children(self) -> int:
        return len(self.entries)


class LonelyRootNode(RecordNode):
    """A Root node that holds records.

    It is an exception for when there is only a single node in the tree.
    """

    __slots__ = ['_node_type_int', 'min_children', 'max_children']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 1
        self.min_children = 0
        self.max_children = tree_conf.order - 1
        super().__init__(tree_conf, data, page, parent)

    def convert_to_leaf(self):
        leaf = LeafNode(self._tree_conf, page=self.page)
        leaf.entries = self.entries
        return leaf


class LeafNode(RecordNode):
    """Node that holds the actual records within the tree."""

    __slots__ = ['_node_type_int', 'min_children', 'max_children']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None, next_page: int=None):
        self._node_type_int = 4
        self.min_children = math.ceil(tree_conf.order / 2) - 1
        self.max_children = tree_conf.order - 1
        super().__init__(tree_conf, data, page, parent, next_page)


class ReferenceNode(Node):

    __slots__ = ['_entry_class']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._entry_class = Reference
        super().__init__(tree_conf, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self.entries) + 1 if self.entries else 0

    def insert_entry(self, entry: 'Reference'):
        """Make sure that after of a reference matches before of the next one.

        Probably very inefficient approach.
        """
        super().insert_entry(entry)
        i = self.entries.index(entry)
        if i > 0:
            previous_entry = self.entries[i-1]
            previous_entry.after = entry.before
        try:
            next_entry = self.entries[i+1]
        except IndexError:
            pass
        else:
            next_entry.before = entry.after


class RootNode(ReferenceNode):
    """The first node at the top of the tree."""

    __slots__ = ['_node_type_int', 'min_children', 'max_children']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 2
        self.min_children = 2
        self.max_children = tree_conf.order
        super().__init__(tree_conf, data, page, parent)

    def convert_to_internal(self):
        internal = InternalNode(self._tree_conf, page=self.page)
        internal.entries = self.entries
        return internal


class InternalNode(ReferenceNode):
    """Node that only holds references to other Internal nodes or Leaves."""

    __slots__ = ['_node_type_int', 'min_children', 'max_children']

    def __init__(self, tree_conf: TreeConf, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 3
        self.min_children = math.ceil(tree_conf.order / 2)
        self.max_children = tree_conf.order
        super().__init__(tree_conf, data, page, parent)
