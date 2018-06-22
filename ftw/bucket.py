import collections

import fdb
fdb.api_version(510)


Item = collections.namedtuple('Item', ['key', 'item', 'value'])


class Bucket:
    def __init__(self, space):
        self.space = space

    def put(self, db, key, item, value):
        return db.set(self.space[key][item], value)

    def get(self, db, key):
        items = db[self.space[key].range()]
        ret = []
        for x in items:
            key, item = self.space.unpack(x.key)
            ret.append(Item(key, item, x.value))
        return ret
