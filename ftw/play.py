#!/usr/bin/env python3


import collections
import struct
import io

import fdb
fdb.api_version(510)


Item = collections.namedtuple('Item', ['key', 'value'])


class Stream:
    def __init__(self, seq, db, name):
        self.seq = seq
        try:
            self.d = fdb.directory.open(db, name)
        except ValueError:
            self.d = self.create(db, name)

    @fdb.transactional
    def create(self, tr, name):
        d = fdb.directory.create(tr, name)
        tr[d['count'].key()] = b'\x00'
        return d

    @fdb.transactional
    def put(self, tr, reader):
        if not hasattr(reader, 'read'):
            reader = io.BytesIO(reader)

        tr.add(self.d['count'].key(), b'\x01')

        n = next(self.seq)
        b = struct.pack('Q', n)

        val = reader.read(10000)
        assert len(val) < 10000

        tr.set(self.d['i'][b].key(), val)

        return

        offset = 0
        while True:
            value = reader.read(10000)
            if not value:
                break
            key = self.space.pack((b, offset))
            tr.set(key, value)
            offset += 1
        return True

    def range(self, db):
        ret = []
        for x in db[self.d['i'].range()]:
            key = self.d['i'].unpack(x.key)[0]
            stamp = struct.unpack('Q', key)
            ret.append(Item(stamp, x.value))
        return ret

    @fdb.transactional
    def count(self, tr):
        return int.from_bytes(
            tr[self.d['count'].key()], byteorder='little')
