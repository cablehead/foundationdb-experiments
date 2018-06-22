from ftw import bucket


def test_bucket(db, space, seq):
    b = bucket.Bucket(space['user_item'])

    b.put(db, b'user1', b'item1', b'bag')
    b.put(db, b'user1', b'item2', b'chair')
    b.put(db, b'user2', b'item3', b'table')

    assert b.get(db, b'user1') == [
        (b'user1', b'item1', b'bag'),
        (b'user1', b'item2', b'chair'), ]

    assert b.get(db, b'user2') == [
        (b'user2', b'item3', b'table'), ]
