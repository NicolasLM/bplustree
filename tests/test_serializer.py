import uuid

from bplustree.serializer import IntSerializer, StrSerializer, UUIDSerializer


def test_int_serializer():
    s = IntSerializer()
    assert s.serialize(42, 2) == b'*\x00'
    assert s.deserialize(b'*\x00') == 42


def test_str_serializer():
    s = StrSerializer()
    assert s.serialize('foo', 3) == b'foo'
    assert s.deserialize(b'foo') == 'foo'


def test_uuid_serializer():
    s = UUIDSerializer()
    id_ = uuid.uuid4()
    assert s.serialize(id_, 16) == id_.bytes
    assert s.deserialize(id_.bytes) == id_
