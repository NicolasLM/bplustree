import abc

from .const import (ENDIAN, PAGE_REFERENCE_BYTES,
                    USED_KEY_LENGTH_BYTES, USED_VALUE_LENGTH_BYTES, TreeConf)


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

    def __init__(self, tree_conf: TreeConf, key=None,
                 value: bytes=None, data: bytes=None):
        self._tree_conf = tree_conf
        self.key = key
        self.value = value
        self.length = (
            USED_KEY_LENGTH_BYTES + self._tree_conf.key_size +
            USED_VALUE_LENGTH_BYTES + self._tree_conf.value_size
        )
        if data:
            self.load(data)
        if self.value:
            assert len(self.value) <= self._tree_conf.value_size

    def load(self, data: bytes):
        assert len(data) == self.length

        end_used_key_length = USED_KEY_LENGTH_BYTES
        used_key_length = int.from_bytes(data[0:end_used_key_length], ENDIAN)
        assert 0 <= used_key_length <= self._tree_conf.key_size

        end_key = end_used_key_length + used_key_length
        self.key = self._tree_conf.serializer.deserialize(
            data[end_used_key_length:end_key]
        )

        start_used_value_length = (
            end_used_key_length + self._tree_conf.key_size
        )
        end_used_value_length = end_key + USED_VALUE_LENGTH_BYTES
        used_value_length = int.from_bytes(
            data[start_used_value_length:end_used_value_length], ENDIAN
        )
        assert 0 <= used_value_length <= self._tree_conf.value_size

        end_value = end_used_value_length + used_value_length
        self.value = data[end_used_value_length:end_value]

    def dump(self) -> bytes:
        assert isinstance(self.value, bytes)
        key_as_bytes = self._tree_conf.serializer.serialize(
            self.key, self._tree_conf.key_size
        )
        used_key_length = len(key_as_bytes)
        used_value_length = len(self.value)
        data = (
            used_key_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN) +
            key_as_bytes +
            bytes(self._tree_conf.key_size - used_key_length) +
            used_value_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN) +
            self.value +
            bytes(self._tree_conf.value_size - used_value_length)
        )
        return data

    def __repr__(self):
        return '<Record: {} value={}>'.format(
            self.key, self.value
        )


class Reference(Entry):
    """A container for a reference to other nodes."""

    def __init__(self, tree_conf: TreeConf, key=None, before=None, after=None,
                 data: bytes=None):
        self._tree_conf = tree_conf
        self.length = (
            2 * PAGE_REFERENCE_BYTES +
            USED_KEY_LENGTH_BYTES +
            self._tree_conf.key_size
        )
        self.key = key
        self.before = before
        self.after = after
        if data:
            self.load(data)

    def load(self, data: bytes):
        assert len(data) == self.length
        end_before = PAGE_REFERENCE_BYTES
        self.before = int.from_bytes(data[0:end_before], ENDIAN)

        end_used_key_length = end_before + USED_KEY_LENGTH_BYTES
        used_key_length = int.from_bytes(
            data[end_before:end_used_key_length], ENDIAN
        )
        assert 0 <= used_key_length <= self._tree_conf.key_size

        end_key = end_used_key_length + used_key_length
        self.key = self._tree_conf.serializer.deserialize(
            data[end_used_key_length:end_key]
        )

        start_after = end_used_key_length + self._tree_conf.key_size
        end_after = end_key + PAGE_REFERENCE_BYTES
        self.after = int.from_bytes(data[start_after:end_after], ENDIAN)

    def dump(self) -> bytes:
        assert isinstance(self.before, int)
        assert isinstance(self.after, int)

        key_as_bytes = self._tree_conf.serializer.serialize(
            self.key, self._tree_conf.key_size
        )
        used_key_length = len(key_as_bytes)

        data = (
            self.before.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN) +
            used_key_length.to_bytes(USED_VALUE_LENGTH_BYTES, ENDIAN) +
            key_as_bytes +
            bytes(self._tree_conf.key_size - used_key_length) +
            self.after.to_bytes(PAGE_REFERENCE_BYTES, ENDIAN)
        )
        return data

    def __repr__(self):
        return '<Reference: key={} before={} after={}>'.format(
            self.key, self.before, self.after
        )
