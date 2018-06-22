import pytest

import fity3

import fdb
fdb.api_version(510)


@pytest.fixture(scope="session")
def db():
    return fdb.open()


@pytest.fixture(scope="session")
def seq():
    return fity3.generator(1)


@pytest.fixture(scope="session")
def space(db, seq):
    name = (str(next(seq)), )
    yield fdb.directory.create_or_open(db, name)
    fdb.directory.remove(db, name)
