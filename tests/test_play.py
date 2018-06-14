import pytest

import fity3

from ftw import play

import fdb
fdb.api_version(510)


@pytest.fixture
def db():
    return fdb.open()


@pytest.fixture
def stream(db):
    f3 = fity3.generator(1)
    name = (str(next(f3)),)
    yield play.Stream(f3, db, name)
    fdb.directory.remove(db, name)


def test_put_small(db, stream):
    assert stream.count(db) == 0
    stream.put(db, b'foo')
    assert stream.count(db) == 1
    assert [x.value for x in stream.range(db)] == [b'foo']

    stream.put(db, b'bar')
    assert stream.count(db) == 2
    assert [x.value for x in stream.range(db)] == [b'foo', b'bar']
