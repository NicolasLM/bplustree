import os
from unittest import mock

import pytest

filename = '/tmp/bplustree-testfile.index'


@pytest.fixture(autouse=True)
def clean_file():
    if os.path.isfile(filename):
        os.unlink(filename)
    if os.path.isfile(filename + '-wal'):
        os.unlink(filename + '-wal')
    yield
    if os.path.isfile(filename):
        os.unlink(filename)
    if os.path.isfile(filename + '-wal'):
        os.unlink(filename + '-wal')


@pytest.fixture(autouse=True)
def patch_fsync():
    mock_fsync = mock.patch('os.fsync')
    mock_fsync.start()
    yield
    mock_fsync.stop()
