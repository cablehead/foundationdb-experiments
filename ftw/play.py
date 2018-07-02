import collections
import datetime
import struct
import io

import fity3

import fdb
fdb.api_version(510)


Item = collections.namedtuple('Item', ['key', 'value'])


class Item(Item):
    @property
    def timestamp(self):
        return fity3.to_timestamp(self.key)

    @property
    def datetime(self):
        return datetime.datetime.fromtimestamp(self.timestamp)


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

    def last(self, db):
        rng = self.space['i'].range()
        res = db.get_range(rng.start, rng.stop, reverse=True, limit=1)
        if not res:
            return
        return self.unpack(db, res[0])

    def unpack(self, db, item):
        # TODO: this should stream out large values
        key = self.space['i'].unpack(item.key)[0]
        stamp = struct.unpack('>Q', key)[0]
        value = item.value
        if not value:
            for part in db[self.space['b'][key].range()]:
                value += part.value
        return Item(stamp, value)

    def range(self, db):
        for x in db[self.space['i'].range()]:
            yield self.unpack(db, x)

    def count(self, db):
        val = db.get(self.space['count'])
        return int.from_bytes(val, byteorder='little')
