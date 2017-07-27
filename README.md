Bplustree
=========

An on-disk B+tree for Python 3.

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

