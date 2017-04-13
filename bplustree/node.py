import abc
import bisect
import math
from typing import Optional

from .const import ENDIAN, NODE_TYPE_BYTES, USED_PAGE_LENGTH_BYTES
from .entry import Entry, Record, Reference


class Node(abc.ABC):

    # Attributes to redefine in inherited classes
    _node_type_int = 0
    _max_children = 0
    _min_children = 0
    _entry_class = None

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._page_size = page_size
        self._order = order
        self.entries = list()
        self.page = page
        self.parent = parent
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self._page_size
        end_header = NODE_TYPE_BYTES + USED_PAGE_LENGTH_BYTES
        used_page_length = int.from_bytes(
            data[NODE_TYPE_BYTES:end_header], ENDIAN
        )
        entry_length = self._entry_class.length
        for start_offset in range(end_header, used_page_length, entry_length):
            entry_data = data[start_offset:start_offset+entry_length]
            self.entries.append(self._entry_class(data=entry_data))

    def dump(self) -> bytearray:
        data = bytearray()
        for record in self.entries:
            data.extend(record.dump())

        used_page_length = len(data) + 4
        assert 0 <= used_page_length < self._page_size
        header = (self._node_type_int.to_bytes(1, ENDIAN) +
                  used_page_length.to_bytes(3, ENDIAN))

        data = bytearray(header) + data

        padding = self._page_size - len(data)
        assert padding >= 0
        data.extend(bytearray(padding))
        assert len(data) == self._page_size

        return data

    @property
    def can_add_entry(self) -> bool:
        return self.num_children < self._max_children

    @property
    def can_delete_entry(self) -> bool:
        return self.num_children > self._min_children

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

    @abc.abstractproperty
    @property
    def num_children(self) -> int:
        pass

    def pop_smallest(self) -> Entry:
        """Remove and return the smallest entry."""
        return self.entries.pop(0)

    def insert_entry(self, entry: Entry):
        assert isinstance(entry, self._entry_class)
        self.entries.append(entry)
        self.entries.sort()

    def get_entry(self, key):
        entry = self._entry_class(key=key)  # Hack to compare and order
        i = bisect.bisect_left(self.entries, entry)
        if i != len(self.entries) and self.entries[i] == entry:
            return self.entries[i]
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
    def from_page_data(cls, page_size: int, order: int, data: bytes,
                       page: int=None) -> 'Node':
        node_type_byte = data[0:NODE_TYPE_BYTES]
        node_type_int = int.from_bytes(node_type_byte, ENDIAN)
        if node_type_int == 1:
            return LonelyRootNode(page_size, order, data, page)
        elif node_type_int == 2:
            return RootNode(page_size, order, data, page)
        elif node_type_int == 3:
            return InternalNode(page_size, order, data, page)
        elif node_type_int == 4:
            return LeafNode(page_size, order, data, page)
        else:
            assert False, 'No Node with type {} exists'.format(node_type_int)

    def __repr__(self):
        return '<{}: page={} entries={}>'.format(
            self.__class__.__name__, self.page, len(self.entries)
        )


class RecordNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._entry_class = Record
        super().__init__(page_size, order, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self.entries)


class LonelyRootNode(RecordNode):
    """A Root node that holds records.

    It is an exception for when there is only a single node in the tree.
    """

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 1
        self._min_children = 0
        self._max_children = order - 1
        super().__init__(page_size, order, data, page, parent)

    def convert_to_leaf(self):
        leaf = LeafNode(self._page_size, self._order, page=self.page)
        leaf.entries = self.entries
        return leaf


class LeafNode(RecordNode):
    """Node that holds the actual records within the tree."""

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 4
        self._min_children = math.ceil(order / 2) - 1
        self._max_children = order - 1
        super().__init__(page_size, order, data, page, parent)


class ReferenceNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._entry_class = Reference
        super().__init__(page_size, order, data, page, parent)

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

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 2
        self._min_children = 2
        self._max_children = order
        super().__init__(page_size, order, data, page, parent)

    def convert_to_internal(self):
        internal = InternalNode(self._page_size, self._order, page=self.page)
        internal.entries = self.entries
        return internal


class InternalNode(ReferenceNode):
    """Node that only holds references to other Internal nodes or Leaves."""

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 3
        self._min_children = math.ceil(order / 2)
        self._max_children = order
        super().__init__(page_size, order, data, page, parent)
