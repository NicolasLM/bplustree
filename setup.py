from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'LICENSE'), encoding='utf-8') as f:
    long_description += f.read()

with open(path.join(here, 'bplustree', 'const.py'), encoding='utf-8') as fp:
    version = dict()
    exec(fp.read(), version)
    version = version['VERSION']

setup(
    name='bplustree',
    version=version,
    description='On-disk B+tree for Python 3',
    long_description=long_description,
    url='https://github.com/NicolasLM/bplustree',
    author='Nicolas Le Manchet',
    author_email='nicolas@lemanchet.fr',
    license='MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'Topic :: Database',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='bplustree B+tree Btree database index',

    packages=find_packages(include=('bplustree', 'bplustree.*')),
    install_requires=[
        'rwlock',
        'cachetools'
    ],

    extras_require={
        'tests': [
            'pytest',
            'pytest-cov',
            'python-coveralls',
            'pycodestyle'
        ],
        'datetime': [
            'temporenc',
        ],
    },
)
