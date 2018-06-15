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
        b = struct.pack('>Q', n)

        val = reader.read(10000)

        if len(val) < 10000:
            tr.set(self.d['i'][b].key(), val)
            return

        tr.set(self.d['i'][b].key(), b'')

        offset = 0
        while val:
            tr.set(self.d['b'][b][offset].key(), val)
            offset += 1
            val = reader.read(10000)

    def range(self, db):
        for x in db[self.d['i'].range()]:
            key = self.d['i'].unpack(x.key)[0]
            stamp = struct.unpack('>Q', key)[0]
            value = x.value
            if not value:
                for part in db[self.d['b'][key].range()]:
                    value += part.value
            yield Item(stamp, value)

    @fdb.transactional
    def count(self, tr):
        return int.from_bytes(
            tr[self.d['count'].key()], byteorder='little')
