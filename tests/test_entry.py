from bplustree.entry import Record, Reference


def test_record_serialization():
    r1 = Record(42, b'foo')
    data = r1.dump()

    r2 = Record(data=data)
    assert r1 == r2
    assert r1.value == r2.value


def test_reference_serialization():
    r1 = Reference(42, 1, 2)
    data = r1.dump()

    r2 = Reference(data=data)
    assert r1 == r2
    assert r1.before == r2.before
    assert r1.after == r2.after
