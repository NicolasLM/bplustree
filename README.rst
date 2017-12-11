Bplustree
=========

.. image:: https://travis-ci.org/NicolasLM/bplustree.svg?branch=master
    :target: https://travis-ci.org/NicolasLM/bplustree
.. image:: https://coveralls.io/repos/github/NicolasLM/bplustree/badge.svg?branch=master
    :target: https://coveralls.io/github/NicolasLM/bplustree?branch=master

An on-disk B+tree for Python 3.

Quickstart
----------

Install Bplustree with pip::

   pip install bplustree

Create a B+tree index stored on a file and use it with:

.. code:: python

    >>> from bplustree import BPlusTree
    >>> tree = BPlusTree(filename='/tmp/bplustree.db', order=50)
    >>> tree.insert(1, b'foo')
    >>> tree.insert(2, b'bar')
    >>> tree.get(1)
    b'foo'
    >>> for i in tree:
    ...     print(i)
    ...
    1
    2
    >>> tree.close()

License
-------

MIT
