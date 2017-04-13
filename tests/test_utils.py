import pytest

from bplustree.utils import pairwise


def test_pairwise():
    l = [0, 1, 2, 3, 4]
    i = pairwise(l)
    assert next(i) == (0, 1)
    assert next(i) == (1, 2)
    assert next(i) == (2, 3)
    assert next(i) == (3, 4)
    with pytest.raises(StopIteration):
        next(i)
