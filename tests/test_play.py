from ftw import play

import fdb
fdb.api_version(510)


def test_stream(db, space, seq):
    stream = play.Stream(db, seq, space['stream'])
    assert stream.count(db) == 0

    assert stream.last(db) is None

    stream.put(db, b'foo')
    assert stream.count(db) == 1
    assert [x.value for x in stream.range(db)] == [b'foo']

    stream = play.Stream(db, seq, space['stream'])
    stream.put(db, b'bar')
    assert stream.count(db) == 2
    assert [x.value for x in stream.range(db)] == [b'foo', b'bar']

    val = ''.join([str(i) for i in range(20000)]).encode('utf-8')
    stream.put(db, val)
    assert stream.count(db) == 3
    assert [x.value for x in stream.range(db)] == [b'foo', b'bar', val]

    last = list(stream.range(db))[-1]
    assert stream.last(db) == last


def test_set_get(db, space, seq):
    stream = play.Stream(db, seq, space['stream'])
    stream.set(db, b'foo', b'bar')
    assert stream.get(db, b'foo') == b'bar'
