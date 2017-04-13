import abc

from .const import ENDIAN, KEY_BYTES, VALUE_BYTES, PAGE_REFERENCE_BYTES


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
    """A container for the actual data the tree stores."""

    length = KEY_BYTES + VALUE_BYTES

    def __init__(self, key=None, value=None, data: bytes=None):
        self.key = key
        self.value = value
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self.length
        end_key = KEY_BYTES
        end_value = end_key + VALUE_BYTES
        self.key = int.from_bytes(data[0:end_key], ENDIAN)
        self.value = int.from_bytes(data[end_key:end_value], ENDIAN)

    def dump(self) -> bytes:
        assert isinstance(self.key, int)
        assert isinstance(self.value, int)
        data = (self.key.to_bytes(KEY_BYTES, ENDIAN)
                + self.value.to_bytes(VALUE_BYTES, ENDIAN))
        return data

    def __repr__(self):
        return '<Record: {} value={}>'.format(
            self.key, self.value
        )


class Reference(Entry):
    """A container for a reference to other nodes."""

    length = 2 * PAGE_REFERENCE_BYTES + KEY_BYTES

    def __init__(self, key=None, before=None, after=None, data: bytes=None):
        self.key = key
        self.before = before
        self.after = after
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self.length
        end_before = PAGE_REFERENCE_BYTES
        end_key = end_before + KEY_BYTES
        end_after = end_key + PAGE_REFERENCE_BYTES
        self.before = int.from_bytes(data[0:end_before], ENDIAN)
        self.key = int.from_bytes(data[end_before:end_key], ENDIAN)
        self.after = int.from_bytes(data[end_key:end_after], ENDIAN)

    def dump(self) -> bytes:
        assert isinstance(self.before, int)
        assert isinstance(self.key, int)
        assert isinstance(self.after, int)
        data = (
            self.before.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN) +
            self.key.to_bytes(KEY_BYTES, ENDIAN) +
            self.after.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
        )
        return data

    def __repr__(self):
        return '<Reference: key={} before={} after={}>'.format(
            self.key, self.before, self.after
        )
