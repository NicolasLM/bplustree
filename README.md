Bplustree
=========

An on-disk B+tree for Python 3.

[![Build Status](https://travis-ci.org/NicolasLM/bplustree.svg?branch=master)](https://travis-ci.org/NicolasLM/bplustree)
[![Coverage Status](https://coveralls.io/repos/github/NicolasLM/bplustree/badge.svg?branch=master)](https://coveralls.io/github/NicolasLM/bplustree?branch=master)

```python
>>> from bplustree import BPlusTree
>>> tree = BPlusTree(filename='/tmp/bplustree.db', order=50)
>>> tree.insert(1, b'foo')
>>> tree.insert(2, b'bar')
>>> tree.get(1)
b'foo'
>>> for i in tree: i
...
1
2
>>> tree.close()
```

