import pytest

from bplustree.utils import pairwise, iter_slice


def test_pairwise():
    i = pairwise([0, 1, 2, 3, 4])
    assert next(i) == (0, 1)
    assert next(i) == (1, 2)
    assert next(i) == (2, 3)
    assert next(i) == (3, 4)
    with pytest.raises(StopIteration):
        next(i)


def test_iter_slice():
    i = iter_slice(b'12345678', 3)
    assert next(i) == (b'123', False)
    assert next(i) == (b'456', False)
    assert next(i) == (b'78', True)
    with pytest.raises(StopIteration):
        next(i)

    i = iter_slice(b'123456', 3)
    assert next(i) == (b'123', False)
    assert next(i) == (b'456', True)
    with pytest.raises(StopIteration):
        next(i)
