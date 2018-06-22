import collections
import struct
import io

import fdb
fdb.api_version(510)


Item = collections.namedtuple('Item', ['key', 'value'])


class Stream:
    @fdb.transactional
    def __init__(self, tr, seq, space):
        self.seq = seq
        self.space = space
        count = tr.get(self.space['count'])
        if not count.present():
            tr.set(self.space['count'], b'\x00')

    def set(self, db, key, value):
        db.set(self.space['a'][key], value)

    def get(self, db, key):
        return db.get(self.space['a'][key])

    @fdb.transactional
    def put(self, tr, reader):
        if not hasattr(reader, 'read'):
            reader = io.BytesIO(reader)

        tr.add(self.space['count'].key(), b'\x01')

        n = next(self.seq)
        b = struct.pack('>Q', n)

        val = reader.read(10000)

        if len(val) < 10000:
            tr.set(self.space['i'][b].key(), val)
            return

        tr.set(self.space['i'][b].key(), b'')

        offset = 0
        while val:
            tr.set(self.space['b'][b][offset].key(), val)
            offset += 1
            val = reader.read(10000)

    def range(self, db):
        for x in db[self.space['i'].range()]:
            key = self.space['i'].unpack(x.key)[0]
            stamp = struct.unpack('>Q', key)[0]
            value = x.value
            if not value:
                for part in db[self.space['b'][key].range()]:
                    value += part.value
            yield Item(stamp, value)

    def count(self, db):
        val = db.get(self.space['count'])
        return int.from_bytes(val, byteorder='little')
