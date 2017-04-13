import abc
import bisect
import copy
from functools import partial
import itertools
import math
import mmap
from typing import Optional, Union, Iterable

ENDIAN = 'little'


class BPlusTree:

    def __init__(self, filename: Optional[str]=None,
                 length: Optional[int]=None, order: int=4):
        if not length:
            length = 5000 * mmap.PAGESIZE
        if not filename:
            self._fd = None
            self._mm = mmap.mmap(-1, length, access=mmap.ACCESS_WRITE)
            self._create_init_values(order, length)
            self._write_metadata()
        else:
            self._fd = open(filename, mode='r+b')
            self._mm = mmap.mmap(self._fd.fileno(), 0)
            self._read_metadata()

        self.LonelyRootNode = partial(LonelyRootNode, self._page_size,
                                      self._order)
        self.RootNode = partial(RootNode, self._page_size, self._order)
        self.InternalNode = partial(InternalNode, self._page_size, self._order)
        self.LeafNode = partial(LeafNode, self._page_size, self._order)
        root_node_data = self.LonelyRootNode().dump()
        self._write_page(self._root_node_page, root_node_data)

    def close(self):
        self._mm.flush()
        self._mm.close()
        if self._fd:
            self._fd.close()

    def _create_init_values(self, order, length):
        self._root_node_page = 1
        self._page_size = mmap.PAGESIZE
        self._order = order
        self._max_page = length / self._page_size
        self._next_available_page = 2

    def _write_page(self, page: int, data: Union[bytes, bytearray]):
        assert 0 < page <= self._max_page
        assert len(data) == self._page_size
        self._mm.seek(page * self._page_size)
        self._mm.write(data)

    def _write_node(self, node: 'Node'):
        data = node.dump()
        self._write_page(node.page, data)

    def _read_page(self, page: int) -> bytes:
        assert 0 < page <= self._max_page
        start = page * self._page_size
        stop = start + self._page_size
        data = self._mm[start:stop]
        assert len(data) == self._page_size
        return data

    def _get_node_at_page(self, page: int) -> 'Node':
        data = self._read_page(page)
        return Node.from_page_data(self._page_size, self._order, data=data,
                                   page=page)

    def _read_metadata(self):
        self._root_node_page = int.from_bytes(self._mm[0:3], ENDIAN)
        self._page_size = int.from_bytes(self._mm[4:7], ENDIAN)
        self._order = int.from_bytes(self._mm[8:11], ENDIAN)

    def _write_metadata(self):
        self._mm.seek(0)
        self._mm.write(self._root_node_page.to_bytes(4, ENDIAN))
        self._mm.write(self._page_size.to_bytes(4, ENDIAN))
        self._mm.write(self._order.to_bytes(4, ENDIAN))

    def _allocate_new_page(self) -> int:
        rv = copy.copy(self._next_available_page)
        self._next_available_page += 1
        self._write_metadata()
        return rv

    @property
    def _root_node(self) -> Union['LonelyRootNode', 'RootNode']:
        root_node = self._get_node_at_page(self._root_node_page)
        assert isinstance(root_node, (LonelyRootNode, RootNode))
        return root_node

    def get(self, key):
        node = self._search_in_tree(key, self._root_node)
        record = node.get_entry(key)
        return record.value

    def insert(self, key, value):
        node = self._search_in_tree(key, self._root_node)
        if node.can_add_entry:
            node.insert_entry(Record(key, value))
            self._write_node(node)
        else:
            node.insert_entry(Record(key, value))
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
            for ref_a, ref_b in pairwise(node._entries):
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
        new_node._entries = new_entries
        ref = Reference(new_node.smallest_key, old_node.page,
                        new_node.page)

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

    def _split_parent(self, old_node: 'Node'):
        parent = old_node.parent
        new_node = self.InternalNode(page=self._allocate_new_page())
        new_entries = old_node.split_entries()
        new_node._entries = new_entries

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

    def _create_new_root(self, reference: 'Reference'):
        new_root = self.RootNode(page=self._allocate_new_page())
        new_root.insert_entry(reference)
        self._root_node_page = new_root.page
        self._write_metadata()
        self._write_node(new_root)


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
        self._entries = list()
        self.page = page
        self.parent = parent
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self._page_size
        used_page_length = int.from_bytes(data[1:4], ENDIAN)
        entry_length = self._entry_class.length
        for start_offset in range(4, used_page_length, entry_length):
            entry_data = data[start_offset:start_offset+entry_length]
            self._entries.append(self._entry_class(data=entry_data))

    def dump(self) -> bytearray:
        data = bytearray()
        for record in self._entries:
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
        return self._entries[0]

    @property
    def biggest_key(self):
        return self.biggest_entry.key

    @property
    def biggest_entry(self):
        return self._entries[-1]

    @abc.abstractproperty
    @property
    def num_children(self) -> int:
        pass

    def pop_smallest(self) -> 'Entry':
        """Remove and return the smallest entry."""
        return self._entries.pop(0)

    def insert_entry(self, entry: 'Entry'):
        assert isinstance(entry, self._entry_class)
        self._entries.append(entry)
        self._entries.sort()

    def get_entry(self, key):
        entry = self._entry_class(key=key)  # Hack to compare and order
        i = bisect.bisect_left(self._entries, entry)
        if i != len(self._entries) and self._entries[i] == entry:
            return self._entries[i]
        raise ValueError('No entry for key {}'.format(key))

    def split_entries(self) -> list:
        """Split the entries in half.

        Keep the lower part in the node and return the upper one.
        """
        len_entries = len(self._entries)
        rv = self._entries[len_entries//2:]
        self._entries = self._entries[:len_entries//2]
        assert len(self._entries) + len(rv) == len_entries
        return rv

    @classmethod
    def from_page_data(cls, page_size: int, order: int, data: bytes,
                       page: int=None) -> 'Node':
        node_type_byte = data[0:1]
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
            raise RuntimeError()
            assert False, 'No Node with type {} exists'.format(node_type_int)

    def __repr__(self):
        return '<{}: page={} entries={}>'.format(
            self.__class__.__name__, self.page, len(self._entries)
        )


class LonelyRootNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 1
        self._entry_class = Record
        self._min_children = 0
        self._max_children = order - 1
        super().__init__(page_size, order, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self._entries)

    def convert_to_leaf(self):
        leaf = LeafNode(self._page_size, self._order, page=self.page)
        leaf._entries = self._entries
        return leaf


class RootNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 2
        self._entry_class = Reference
        self._min_children = 2
        self._max_children = order
        super().__init__(page_size, order, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self._entries) + 1 if self._entries else 0

    def convert_to_internal(self):
        internal = InternalNode(self._page_size, self._order, page=self.page)
        internal._entries = self._entries
        return internal

    def insert_entry(self, entry: 'Entry'):
        super().insert_entry(entry)
        i = self._entries.index(entry)
        if i > 0:
            previous_entry = self._entries[i-1]
            previous_entry.after = entry.before
        try:
            next_entry = self._entries[i+1]
        except IndexError:
            pass
        else:
            next_entry.before = entry.after


class InternalNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 3
        self._entry_class = Reference
        self._min_children = math.ceil(order / 2)
        self._max_children = order
        super().__init__(page_size, order, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self._entries) + 1 if self._entries else 0

    def insert_entry(self, entry: 'Entry'):
        super().insert_entry(entry)
        i = self._entries.index(entry)
        if i > 0:
            previous_entry = self._entries[i-1]
            previous_entry.after = entry.before
        try:
            next_entry = self._entries[i+1]
        except IndexError:
            pass
        else:
            next_entry.before = entry.after


class LeafNode(Node):

    def __init__(self, page_size: int, order: int, data: Optional[bytes]=None,
                 page: int=None, parent: 'Node'=None):
        self._node_type_int = 4
        self._entry_class = Record
        self._min_children = math.ceil(order / 2) - 1
        self._max_children = order - 1
        super().__init__(page_size, order, data, page, parent)

    @property
    def num_children(self) -> int:
        return len(self._entries)


class Entry(abc.ABC):

    key = None

    @abc.abstractmethod
    def load(self, data: bytes):
        pass

    @abc.abstractmethod
    def dump(self) -> bytes:
        pass

    def __eq__(self, other):
        return self.key == other.key

    def __lt__(self, other):
        return self.key < other.key

    def __le__(self, other):
        return self.key <= other.key

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key


class Record(Entry):

    length = 16

    def __init__(self, key=None, value=None, data: bytes=None):
        self.key = key
        self.value = value
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == 16
        self.key = int.from_bytes(data[0:8], ENDIAN)
        self.value = int.from_bytes(data[8:16], ENDIAN)

    def dump(self) -> bytes:
        assert isinstance(self.key, int)
        assert isinstance(self.value, int)
        data = self.key.to_bytes(8, ENDIAN) + self.value.to_bytes(8, ENDIAN)
        return data

    def __repr__(self):
        return '<Record: {} value={}>'.format(
            self.key, self.value
        )


class Reference(Entry):

    length = 24

    def __init__(self, key=None, before=None, after=None, data: bytes=None):
        self.key = key
        self.before = before
        self.after = after
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == 24
        self.before = int.from_bytes(data[0:8], ENDIAN)
        self.key = int.from_bytes(data[8:16], ENDIAN)
        self.after = int.from_bytes(data[16:24], ENDIAN)

    def dump(self) -> bytes:
        assert isinstance(self.before, int)
        assert isinstance(self.key, int)
        assert isinstance(self.after, int)
        data = (
            self.before.to_bytes(8, ENDIAN) +
            self.key.to_bytes(8, ENDIAN) +
            self.after.to_bytes(8, ENDIAN)
        )
        return data

    def __repr__(self):
        return '<Reference: key={} before={} after={}>'.format(
            self.key, self.before, self.after
        )


def pairwise(iterable: Iterable):
    """Iterate over elements two by two.

    s -> (s0,s1), (s1,s2), (s2, s3), ...
    """
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)
