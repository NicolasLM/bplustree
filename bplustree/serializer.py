from abc import ABC, abstractmethod

from .const import ENDIAN


class Serializer(ABC):

    @abstractmethod
    def serialize(self, obj: object, key_size: int) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> object:
        pass


class IntSerializer(Serializer):

    def serialize(self, obj: int, key_size: int) -> bytes:
        return obj.to_bytes(key_size, ENDIAN)

    def deserialize(self, data: bytes) -> int:
        return int.from_bytes(data, ENDIAN)


class StrSerializer(Serializer):

    def serialize(self, obj: str, key_size: int) -> bytes:
        rv = obj.encode(encoding='utf-8')
        assert len(rv) <= key_size
        return rv

    def deserialize(self, data: bytes) -> str:
        return data.decode(encoding='utf-8')
