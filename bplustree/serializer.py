import abc
from datetime import datetime, timezone
from uuid import UUID

try:
    import temporenc
except ImportError:
    temporenc = None

from .const import ENDIAN


class Serializer(metaclass=abc.ABCMeta):

    __slots__ = []

    @abc.abstractmethod
    def serialize(self, obj: object, key_size: int) -> bytes:
        """Serialize a key to bytes."""

    @abc.abstractmethod
    def deserialize(self, data: bytes) -> object:
        """Create a key object from bytes."""

    def __repr__(self):
        return '{}()'.format(self.__class__.__name__)


class IntSerializer(Serializer):

    __slots__ = []

    def serialize(self, obj: int, key_size: int) -> bytes:
        return obj.to_bytes(key_size, ENDIAN)

    def deserialize(self, data: bytes) -> int:
        return int.from_bytes(data, ENDIAN)


class StrSerializer(Serializer):

    __slots__ = []

    def serialize(self, obj: str, key_size: int) -> bytes:
        rv = obj.encode(encoding='utf-8')
        assert len(rv) <= key_size
        return rv

    def deserialize(self, data: bytes) -> str:
        return data.decode(encoding='utf-8')


class UUIDSerializer(Serializer):

    __slots__ = []

    def serialize(self, obj: UUID, key_size: int) -> bytes:
        return obj.bytes

    def deserialize(self, data: bytes) -> UUID:
        return UUID(bytes=data)


class DatetimeUTCSerializer(Serializer):

    __slots__ = []

    def __init__(self):
        if temporenc is None:
            raise RuntimeError('Serialization to/from datetime needs the '
                               'third-party library "temporenc"')

    def serialize(self, obj: datetime, key_size: int) -> bytes:
        if obj.tzinfo is None:
            raise ValueError('DatetimeUTCSerializer needs a timezone aware '
                             'datetime')
        return temporenc.packb(obj, type='DTS')

    def deserialize(self, data: bytes) -> datetime:
        rv = temporenc.unpackb(data).datetime()
        rv = rv.replace(tzinfo=timezone.utc)
        return rv
