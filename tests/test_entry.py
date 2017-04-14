from bplustree.entry import Record, Reference
from bplustree.const import TreeConf

tree_conf = TreeConf(4096, 4, 16, 16)


def test_record_serialization():
    r1 = Record(tree_conf, 42, b'foo')
    data = r1.dump()

    r2 = Record(tree_conf, data=data)
    assert r1 == r2
    assert r1.value == r2.value


def test_reference_serialization():
    r1 = Reference(tree_conf, 42, 1, 2)
    data = r1.dump()

    r2 = Reference(tree_conf, data=data)
    assert r1 == r2
    assert r1.before == r2.before
    assert r1.after == r2.after
