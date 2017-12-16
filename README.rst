Bplustree
=========

.. image:: https://travis-ci.org/NicolasLM/bplustree.svg?branch=master
    :target: https://travis-ci.org/NicolasLM/bplustree
.. image:: https://coveralls.io/repos/github/NicolasLM/bplustree/badge.svg?branch=master
    :target: https://coveralls.io/github/NicolasLM/bplustree?branch=master

An on-disk B+tree for Python 3.

It feels like a dict, but stored on disk. When to use it?

- When the data to store does not fit in memory
- When the data needs to be persisted
- When keeping the keys in order is important

Quickstart
----------

Install Bplustree with pip::

   pip install bplustree

Create a B+tree index stored on a file and use it with:

.. code:: python

    >>> from bplustree import BPlusTree
    >>> tree = BPlusTree('/tmp/bplustree.db', order=50)
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

Keys and values
---------------

Keys must have a natural order and must be serializable to bytes. Some default
serializers for the most common types are provided. For example to index UUIDs:

.. code:: python

    >>> import uuid
    >>> from bplustree import BPlusTree, UUIDSerializer
    >>> tree = BPlusTree('/tmp/bplustree.db', serializer=UUIDSerializer(), key_size=16)
    >>> tree.insert(uuid.uuid4(), b'foo')
    >>> list(tree.keys())
    [UUID('48f2553c-de23-4d20-95bf-6972a89f3bc0')]

Values on the other hand are always bytes. Like keys, the limit on their length
can be set with ``value_size=128`` when building the tree.

Concurrency
-----------

The tree is thread-safe, it follows the multiple readers/single writer pattern.

It is safe to:

- Share an instance of a ``BPlusTree`` between multiple threads

It is NOT safe to:

- Share an instance of a ``BPlusTree`` between multiple processes
- Create multiple instances of ``BPlusTree`` pointing to the same file

Durability
----------

A write-ahead log (WAL) is used to ensure that the data is safe. All changes
made to the tree are appended to the WAL and only merged into the tree in an
operation called a checkpoint, usually when the tree is closed. This approach
is heavily inspired by other databases like SQLite.

If tree doesn't get closed properly (power outage, process killed...) the WAL
file is merged the next time the tree is opened.

License
-------

MIT
